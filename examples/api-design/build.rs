fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/showcase.proto");
    vaelo_build::configure().compile_protos(&["proto/showcase.proto"], &["proto"])?;
    Ok(())
}
