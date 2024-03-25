fn main() {
    tonic_build::compile_protos("./echo.proto").unwrap()
}
