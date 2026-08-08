// Copyright 2024-2025 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::fmt::Debug;
use core::time::Duration;
use std::time::Instant;

use futures::Stream;
use redis::aio::ConnectionLike;
use redis::{Arg, ErrorKind, RedisError, Value};
use tracing::{error, warn};

use crate::redis_utils::aggregate_types::RedisCursorData;
use crate::redis_utils::ft_cursor_read::ft_cursor_read;

/// How to walk a result set larger than one reply.
///
/// The two arms are not interchangeable tuning choices — they are dictated by
/// the server. `RediSearch` has server-side cursors; valkey-search has no
/// `FT.CURSOR` at all, so there it is `LIMIT` or nothing.
#[derive(Debug)]
pub(crate) enum FtAggregatePaging {
    /// `WITHCURSOR COUNT <count> MAXIDLE <max_idle>`, walked with
    /// `FT.CURSOR READ`. `RediSearch` only.
    Cursor { count: u64, max_idle: u64 },

    /// `LIMIT <offset> <page_size>`, walked by re-issuing the whole query with
    /// an advancing offset.
    ///
    /// Unlike a cursor this is not a snapshot: a document written between two
    /// pages can shift across the boundary and be seen twice or missed. Both
    /// callers re-run on a timer and treat a missed row as "next tick", so the
    /// exposure is one poll interval. `SORTBY` on a near-monotonic key keeps
    /// the ordering stable enough that this is rare in practice.
    Limit { page_size: u64 },
}

/// Which fields to pull back with each row.
#[derive(Debug)]
pub(crate) enum FtLoad {
    /// `LOAD <n> <field>…` — only valid for fields that are in the index
    /// schema on valkey-search, which is why the payload cannot be named there.
    Named(Vec<String>),

    /// `LOAD *` — every field on the hash.
    ///
    /// Required on valkey-search: it rejects a named non-indexed field with
    /// ``Index field `data` does not exist``, and `data`/`version` are
    /// deliberately not indexed. `LOAD *` returns them fine. The cost is that
    /// the indexed fields ride along too, so the row decoder must ignore
    /// anything that is not the payload.
    All,
}

#[derive(Debug)]
pub(crate) struct FtAggregateOptions {
    pub load: FtLoad,
    pub paging: FtAggregatePaging,
    pub sort_by: Vec<String>,
}

/// Backstop on `Limit` paging so a server that always returns a full page
/// cannot spin forever. At the default page size of 1500 this is ~96k rows,
/// far beyond any real scheduler queue.
const MAX_LIMIT_PAGES: u64 = 64;

/// Per-query `FT.AGGREGATE` timeout in milliseconds.
///
/// `RediSearch`'s module default (≈500 ms) is far too tight for the
/// scheduler's awaited-action index under any meaningful load: queries
/// time out, `NativeLink` surfaces them as parse errors, and the dedup
/// lookup fails. When dedup fails the scheduler creates a duplicate
/// operation for an action that is already in flight — observed as
/// "two same actions running on different PRs" with each running the
/// full `max_action_executing_timeout_s` window before completing. Pass an
/// explicit value generous enough to absorb 1M+ document scans on a
/// busy `RediSearch` instance.
///
/// valkey-search also accepts `TIMEOUT` (probed against the live dev
/// `ElastiCache`), so it lives in the shared prefix rather than behind the
/// `Cursor` arm.
const FT_AGGREGATE_TIMEOUT_MS: u64 = 10_000;

/// Build one `FT.AGGREGATE` invocation.
///
/// Argument order differs per paging mode on purpose. The cursor form keeps
/// the historical `LOAD … WITHCURSOR … SORTBY` order that `RediSearch` has always
/// accepted here; the limit form uses the canonical `LOAD … SORTBY … LIMIT`
/// order that valkey-search's parser expects.
fn build_aggregate_cmd(
    index: &str,
    query: &str,
    load: &FtLoad,
    sort_by: &[String],
    paging: &FtAggregatePaging,
    offset: u64,
) -> redis::Cmd {
    let mut cmd = redis::cmd("FT.AGGREGATE");
    cmd.arg(index)
        .arg(query)
        .arg("TIMEOUT")
        .arg(FT_AGGREGATE_TIMEOUT_MS)
        .arg("LOAD");
    match load {
        FtLoad::All => {
            cmd.arg("*");
        }
        FtLoad::Named(fields) => {
            cmd.arg(fields.len());
            for field in fields {
                cmd.arg(field);
            }
        }
    }

    let sort_by_args = |cmd: &mut redis::Cmd| {
        if sort_by.is_empty() {
            return;
        }
        cmd.arg("SORTBY").arg(sort_by.len() * 2);
        for key in sort_by {
            cmd.arg(key).arg("ASC");
        }
    };

    match paging {
        FtAggregatePaging::Cursor { count, max_idle } => {
            cmd.arg("WITHCURSOR")
                .arg("COUNT")
                .arg(*count)
                .arg("MAXIDLE")
                .arg(*max_idle);
            sort_by_args(&mut cmd);
        }
        FtAggregatePaging::Limit { page_size } => {
            sort_by_args(&mut cmd);
            cmd.arg("LIMIT").arg(offset).arg(*page_size);
        }
    }
    cmd
}

fn describe_args(cmd: &redis::Cmd) -> Vec<String> {
    cmd.args_iter()
        .map(|a| match a {
            Arg::Simple(bytes) => match str::from_utf8(bytes) {
                Ok(s) => s.to_string(),
                Err(_) => format!("{bytes:?}"),
            },
            other => format!("{other:?}"),
        })
        .collect()
}

/// Calls `FT.AGGREGATE` in redis. redis-rs does not properly support this command
/// so we have to manually handle it.
pub(crate) async fn ft_aggregate<C>(
    mut connection_manager: C,
    index: String,
    query: String,
    options: FtAggregateOptions,
) -> Result<impl Stream<Item = Result<Value, RedisError>> + Send, RedisError>
where
    C: ConnectionLike + Send,
{
    struct State<C: ConnectionLike> {
        connection_manager: C,
        index: String,
        query: String,
        options: FtAggregateOptions,
        data: RedisCursorData,
        aggregate_start: Instant,
        first_batch_ms: u128,
        rounds: u64,
        /// Rows already yielded; also the `LIMIT` offset of the next page.
        offset: u64,
        /// Set once a page comes back short, or the cursor reaches 0.
        exhausted: bool,
    }

    let first_cmd = build_aggregate_cmd(
        &index,
        &query,
        &options.load,
        &options.sort_by,
        &options.paging,
        0,
    );
    let aggregate_start = Instant::now();
    let res = first_cmd
        .clone()
        .query_async::<Value>(&mut connection_manager)
        .await;
    let first_batch_ms = aggregate_start.elapsed().as_millis();
    let data = match res {
        Ok(d) => d,
        Err(e) => {
            error!(
                ?e,
                index,
                ?query,
                options = ?options,
                all_args = ?describe_args(&first_cmd),
                "Error calling ft.aggregate"
            );
            return Err(e);
        }
    };

    let data = parse_aggregate_reply(data, &options.paging)?;
    let page_size = match options.paging {
        FtAggregatePaging::Cursor { .. } => 0,
        FtAggregatePaging::Limit { page_size } => page_size,
    };
    let first_len = data.data.len() as u64;
    let exhausted = match options.paging {
        // The cursor arm decides via `data.cursor` in the loop below.
        FtAggregatePaging::Cursor { .. } => false,
        FtAggregatePaging::Limit { .. } => first_len < page_size,
    };

    let state = State {
        connection_manager,
        index,
        query,
        options,
        data,
        aggregate_start,
        first_batch_ms,
        rounds: 0,
        offset: first_len,
        exhausted,
    };

    Ok(futures::stream::unfold(
        Some(state),
        move |maybe_state| async move {
            let mut state = maybe_state?;
            loop {
                if let Some(map) = state.data.data.pop_front() {
                    return Some((Ok(map), Some(state)));
                }

                let done = match state.options.paging {
                    FtAggregatePaging::Cursor { .. } => state.data.cursor == 0,
                    FtAggregatePaging::Limit { .. } => {
                        state.exhausted || state.rounds >= MAX_LIMIT_PAGES
                    }
                };
                if done {
                    if state.rounds >= MAX_LIMIT_PAGES {
                        warn!(
                            index = %state.index,
                            rows_returned = state.offset,
                            total_results = state.data.total,
                            "ft_aggregate LIMIT paging hit MAX_LIMIT_PAGES; result set truncated"
                        );
                    }
                    let total_elapsed = state.aggregate_start.elapsed();
                    if total_elapsed > Duration::from_millis(500) {
                        warn!(
                            index = %state.index,
                            ft_aggregate_first_batch_ms = state.first_batch_ms as u64,
                            ft_aggregate_rounds = state.rounds,
                            ft_aggregate_total_ms = total_elapsed.as_millis() as u64,
                            "Slow ft_aggregate"
                        );
                    }
                    return None;
                }

                let data_res = match state.options.paging {
                    FtAggregatePaging::Cursor { .. } => ft_cursor_read(
                        &mut state.connection_manager,
                        state.index.clone(),
                        state.data.cursor,
                    )
                    .await,
                    FtAggregatePaging::Limit { page_size } => {
                        let cmd = build_aggregate_cmd(
                            &state.index,
                            &state.query,
                            &state.options.load,
                            &state.options.sort_by,
                            &state.options.paging,
                            state.offset,
                        );
                        match cmd.query_async::<Value>(&mut state.connection_manager).await {
                            Ok(v) => parse_aggregate_reply(v, &state.options.paging).inspect(
                                |parsed| {
                                    let got = parsed.data.len() as u64;
                                    state.offset += got;
                                    state.exhausted = got < page_size;
                                },
                            ),
                            Err(e) => {
                                error!(
                                    ?e,
                                    index = %state.index,
                                    offset = state.offset,
                                    all_args = ?describe_args(&cmd),
                                    "Error paging ft.aggregate with LIMIT"
                                );
                                Err(e)
                            }
                        }
                    }
                };
                state.rounds += 1;
                state.data = match data_res {
                    Ok(data) => data,
                    Err(err) => return Some((Err(err), None)),
                };
            }
        },
    ))
}

/// Turn one `FT.AGGREGATE` reply into [`RedisCursorData`].
///
/// The two paging modes produce different top-level shapes and both are
/// `Value::Array` under RESP2, so we dispatch on the mode we asked for rather
/// than sniffing:
///
/// - `WITHCURSOR` → `[ <results>, <cursor-id> ]`
/// - `LIMIT`      → `<results>` on its own, with no cursor element
fn parse_aggregate_reply(
    raw_value: Value,
    paging: &FtAggregatePaging,
) -> Result<RedisCursorData, RedisError> {
    match paging {
        FtAggregatePaging::Cursor { .. } => RedisCursorData::try_from(raw_value),
        FtAggregatePaging::Limit { .. } => {
            let mut output = RedisCursorData::default();
            parse_results_value(&mut output, raw_value)?;
            // No server-side cursor exists; paging is driven by the offset.
            output.cursor = 0;
            Ok(output)
        }
    }
}

fn parse_results_value(
    output: &mut RedisCursorData,
    raw_value: Value,
) -> Result<(), RedisError> {
    match raw_value {
        Value::Array(d) => resp2_data_parse(output, &d),
        Value::Map(d) => resp3_data_parse(output, &d),
        other => {
            error!(?other, "Bad data in ft.aggregate, expected array or map");
            Err(RedisError::from((
                ErrorKind::Parse,
                "Non map item",
                format!("{other:?}"),
            )))
        }
    }
}

fn resp2_data_parse(
    output: &mut RedisCursorData,
    results_array: &[Value],
) -> Result<(), RedisError> {
    let mut results_iter = results_array.iter();
    match results_iter.next() {
        Some(Value::Int(t)) => {
            output.total = *t;
        }
        Some(other) => {
            error!(?other, "Non-int for first value in ft.aggregate");
            return Err(RedisError::from((
                ErrorKind::Parse,
                "Non int for aggregate total",
                format!("{other:?}"),
            )));
        }
        None => {
            error!("No items in results array for ft.aggregate!");
            return Err(RedisError::from((
                ErrorKind::Parse,
                "No items in results array for ft.aggregate",
            )));
        }
    }

    for item in results_iter {
        match item {
            Value::Array(items) if items.len() % 2 == 0 => {}
            other => {
                error!(
                    ?other,
                    "Expected an array with an even number of items, didn't get it for aggregate value"
                );
                return Err(RedisError::from((
                    ErrorKind::Parse,
                    "Expected an array with an even number of items, didn't get it for aggregate value",
                    format!("{other:?}"),
                )));
            }
        }

        output.data.push_back(item.clone());
    }
    Ok(())
}

fn resp3_data_parse(
    output: &mut RedisCursorData,
    results_map: &Vec<(Value, Value)>,
) -> Result<(), RedisError> {
    for (raw_key, value) in results_map {
        let Value::SimpleString(key) = raw_key else {
            return Err(RedisError::from((
                ErrorKind::Parse,
                "Expected SimpleString keys",
                format!("{raw_key:?}"),
            )));
        };
        match key.as_str() {
            "attributes" => {
                let Value::Array(attributes) = value else {
                    return Err(RedisError::from((
                        ErrorKind::Parse,
                        "Expected array for attributes",
                        format!("{value:?}"),
                    )));
                };
                if !attributes.is_empty() {
                    return Err(RedisError::from((
                        ErrorKind::Parse,
                        "Expected empty attributes",
                        format!("{attributes:?}"),
                    )));
                }
            }
            "format" => {
                let Value::SimpleString(format) = value else {
                    return Err(RedisError::from((
                        ErrorKind::Parse,
                        "Expected SimpleString for format",
                        format!("{value:?}"),
                    )));
                };
                if format.as_str() != "STRING" {
                    return Err(RedisError::from((
                        ErrorKind::Parse,
                        "Expected STRING format",
                        format.clone(),
                    )));
                }
            }
            "results" => {
                let Value::Array(values) = value else {
                    return Err(RedisError::from((
                        ErrorKind::Parse,
                        "Expected Array for results",
                        format!("{value:?}"),
                    )));
                };
                for raw_value in values {
                    let Value::Map(value) = raw_value else {
                        return Err(RedisError::from((
                            ErrorKind::Parse,
                            "Expected list of maps in result",
                            format!("{raw_value:?}"),
                        )));
                    };
                    for (raw_map_key, raw_map_value) in value {
                        let Value::SimpleString(map_key) = raw_map_key else {
                            return Err(RedisError::from((
                                ErrorKind::Parse,
                                "Expected SimpleString keys for result maps",
                                format!("{raw_key:?}"),
                            )));
                        };
                        match map_key.as_str() {
                            "extra_attributes" => {
                                let extra_attributes_values = match raw_map_value {
                                    Value::Map(extra_attributes_values) => extra_attributes_values,
                                    // A document that expired or was deleted between
                                    // the search phase and the load phase comes back
                                    // as a row with Nil attributes. Under load this is
                                    // routine — completed awaited-action records expire
                                    // constantly — so drop the row instead of failing
                                    // the whole aggregate. Failing here surfaced to
                                    // clients as `INVALID_ARGUMENT`, which Bazel treats
                                    // as permanent, so a single expiry race killed the
                                    // build.
                                    Value::Nil => continue,
                                    other => {
                                        return Err(RedisError::from((
                                            ErrorKind::Parse,
                                            "Expected Map for extra_attributes",
                                            format!("{other:?}"),
                                        )));
                                    }
                                };
                                let mut output_array = vec![];
                                for (e_key, e_value) in extra_attributes_values {
                                    output_array.push(e_key.clone());
                                    output_array.push(e_value.clone());
                                }
                                output.data.push_back(Value::Array(output_array));
                            }
                            "values" => {
                                let Value::Array(values_values) = raw_map_value else {
                                    return Err(RedisError::from((
                                        ErrorKind::Parse,
                                        "Expected Array for values",
                                        format!("{raw_map_value:?}"),
                                    )));
                                };
                                if !values_values.is_empty() {
                                    return Err(RedisError::from((
                                        ErrorKind::Parse,
                                        "Expected empty values (all in extra_attributes)",
                                        format!("{values_values:?}"),
                                    )));
                                }
                            }
                            _ => {
                                return Err(RedisError::from((
                                    ErrorKind::Parse,
                                    "Unknown result map key",
                                    format!("{map_key:?}"),
                                )));
                            }
                        }
                    }
                }
            }
            "total_results" => {
                let Value::Int(total) = value else {
                    return Err(RedisError::from((
                        ErrorKind::Parse,
                        "Expected int for total_results",
                        format!("{value:?}"),
                    )));
                };
                output.total = *total;
            }
            "warning" => {
                let Value::Array(warnings) = value else {
                    return Err(RedisError::from((
                        ErrorKind::Parse,
                        "Expected Array for warning",
                        format!("{value:?}"),
                    )));
                };
                if !warnings.is_empty() {
                    return Err(RedisError::from((
                        ErrorKind::Parse,
                        "Expected empty warnings",
                        format!("{warnings:?}"),
                    )));
                }
            }
            // valkey-search is a separate implementation and may add reply
            // fields RediSearch never sent. An unrecognized informational key
            // is not a reason to fail a scheduler poll.
            other => {
                warn!(key = %other, ?value, "Ignoring unknown key in ft.aggregate reply");
            }
        }
    }
    Ok(())
}

impl TryFrom<Value> for RedisCursorData {
    type Error = RedisError;
    fn try_from(raw_value: Value) -> Result<Self, RedisError> {
        let Value::Array(value) = raw_value else {
            error!(
                ?raw_value,
                "Bad data in ft.aggregate, expected array at top-level"
            );
            return Err(RedisError::from((ErrorKind::Parse, "Expected array")));
        };
        if value.len() < 2 {
            return Err(RedisError::from((
                ErrorKind::Parse,
                "Expected at least 2 elements",
            )));
        }
        let mut output = Self::default();
        let mut value = value.into_iter();
        parse_results_value(&mut output, value.next().unwrap())?;
        let Value::Int(cursor) = value.next().unwrap() else {
            return Err(RedisError::from((
                ErrorKind::Parse,
                "Expected integer as last element",
            )));
        };
        output.cursor = cursor as u64;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load() -> FtLoad {
        FtLoad::Named(vec!["data".to_string(), "version".to_string()])
    }

    fn args_of(cmd: &redis::Cmd) -> Vec<String> {
        describe_args(cmd)
    }

    /// The `RediSearch` wire form must not drift — it is the path in production
    /// today and the one `redis_store_test.rs` exercises against a real module.
    #[test]
    fn builds_cursor_wire_form() {
        let cmd = build_aggregate_cmd(
            "idx",
            "@state:{ queued }",
            &load(),
            &["@sort_key".to_string()],
            &FtAggregatePaging::Cursor {
                count: 1500,
                max_idle: 30_000,
            },
            0,
        );
        assert_eq!(
            args_of(&cmd),
            vec![
                "FT.AGGREGATE",
                "idx",
                "@state:{ queued }",
                "TIMEOUT",
                "10000",
                "LOAD",
                "2",
                "data",
                "version",
                "WITHCURSOR",
                "COUNT",
                "1500",
                "MAXIDLE",
                "30000",
                "SORTBY",
                "2",
                "@sort_key",
                "ASC",
            ]
        );
    }

    /// valkey-search form: SORTBY before LIMIT, no cursor clause, and the
    /// offset advances per page.
    #[test]
    fn builds_limit_wire_form_with_offset() {
        let cmd = build_aggregate_cmd(
            "idx",
            "@state:{ queued }",
            &load(),
            &["@sort_key".to_string()],
            &FtAggregatePaging::Limit { page_size: 1500 },
            3000,
        );
        assert_eq!(
            args_of(&cmd),
            vec![
                "FT.AGGREGATE",
                "idx",
                "@state:{ queued }",
                "TIMEOUT",
                "10000",
                "LOAD",
                "2",
                "data",
                "version",
                "SORTBY",
                "2",
                "@sort_key",
                "ASC",
                "LIMIT",
                "3000",
                "1500",
            ]
        );
    }

    /// The exact command issued against valkey-search. Verified by hand
    /// against `ElastiCache` Valkey 9.1: naming `data`/`version` here fails with
    /// ``Index field `data` does not exist`` because they are not in the index
    /// schema, so `LOAD *` is load-bearing rather than a shortcut.
    #[test]
    fn builds_valkey_search_wire_form() {
        let cmd = build_aggregate_cmd(
            "aa__state_sort_key_3e762c15",
            "@state:{ queued }",
            &FtLoad::All,
            &["@sort_key".to_string()],
            &FtAggregatePaging::Limit { page_size: 1500 },
            0,
        );
        assert_eq!(
            args_of(&cmd),
            vec![
                "FT.AGGREGATE",
                "aa__state_sort_key_3e762c15",
                "@state:{ queued }",
                "TIMEOUT",
                "10000",
                "LOAD",
                "*",
                "SORTBY",
                "2",
                "@sort_key",
                "ASC",
                "LIMIT",
                "0",
                "1500",
            ]
        );
    }

    #[test]
    fn builds_limit_wire_form_without_sort_key() {
        let cmd = build_aggregate_cmd(
            "idx",
            "*",
            &FtLoad::Named(vec!["data".to_string()]),
            &[],
            &FtAggregatePaging::Limit { page_size: 10 },
            0,
        );
        assert_eq!(
            args_of(&cmd),
            vec![
                "FT.AGGREGATE",
                "idx",
                "*",
                "TIMEOUT",
                "10000",
                "LOAD",
                "1",
                "data",
                "LIMIT",
                "0",
                "10"
            ]
        );
    }

    /// A `LIMIT` reply has no trailing cursor element. Parsing it as the
    /// cursor shape would consume the first row as the results array and the
    /// second as a cursor id.
    #[test]
    fn parses_cursorless_resp2_reply() {
        let reply = Value::Array(vec![
            Value::Int(2),
            Value::Array(vec![
                Value::SimpleString("data".into()),
                Value::SimpleString("a".into()),
            ]),
            Value::Array(vec![
                Value::SimpleString("data".into()),
                Value::SimpleString("b".into()),
            ]),
        ]);
        let parsed =
            parse_aggregate_reply(reply, &FtAggregatePaging::Limit { page_size: 1500 }).unwrap();
        assert_eq!(parsed.total, 2);
        assert_eq!(parsed.cursor, 0);
        assert_eq!(parsed.data.len(), 2);
    }

    #[test]
    fn parses_cursor_resp2_reply() {
        let reply = Value::Array(vec![
            Value::Array(vec![
                Value::Int(1),
                Value::Array(vec![
                    Value::SimpleString("data".into()),
                    Value::SimpleString("a".into()),
                ]),
            ]),
            Value::Int(42),
        ]);
        let parsed = parse_aggregate_reply(
            reply,
            &FtAggregatePaging::Cursor {
                count: 10,
                max_idle: 30_000,
            },
        )
        .unwrap();
        assert_eq!(parsed.total, 1);
        assert_eq!(parsed.cursor, 42);
        assert_eq!(parsed.data.len(), 1);
    }

    /// An unfamiliar informational key must not fail the poll.
    #[test]
    fn resp3_tolerates_unknown_keys() {
        let mut output = RedisCursorData::default();
        let map = vec![
            (
                Value::SimpleString("total_results".into()),
                Value::Int(7),
            ),
            (
                Value::SimpleString("some_new_valkey_field".into()),
                Value::Int(1),
            ),
        ];
        resp3_data_parse(&mut output, &map).unwrap();
        assert_eq!(output.total, 7);
    }
}
