#[cfg(feature = "dev-schema")]
fn main() {
    use nativelink_config::cas_server::CasConfig;
    use schemars::schema_for;

    let schema = schema_for!(CasConfig);
    serde_json::to_writer_pretty(std::io::stdout(), &schema).expect("to write schema");
}

#[cfg(not(feature = "dev-schema"))]
fn main() {
    eprintln!("Enable with --features dev-schema");
}
