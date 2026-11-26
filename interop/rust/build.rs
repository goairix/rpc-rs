fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .file_descriptor_set_path(out.join("echo_descriptor.bin"))
        .compile_protos(&["../proto/echo.proto"], &["../proto"])?;
    Ok(())
}
