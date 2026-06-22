use protox::Compiler;
use std::fs;
use std::path::Path;

fn collect_protos(dir: &Path, paths: &mut Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let path = entry.path();
            if path.extension().unwrap_or_default() == "proto" {
                paths.push(format!("{}", path.display()));
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dirs: &[&str] = &["proto/operational", "proto/columnar"];

    let mut paths = vec![];
    for dir in proto_dirs {
        collect_protos(Path::new(dir), &mut paths)?;
    }

    let out_dir = "./src/proto";
    fs::create_dir_all(out_dir)?;

    let file_descriptors = Compiler::new(proto_dirs.iter().copied())?
        .include_imports(true)
        .include_source_info(false)
        .open_files(paths)?
        .file_descriptor_set();

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .include_file("mod.rs")
        .out_dir(out_dir)
        .compile_fds(file_descriptors)?;

    println!("cargo::rerun-if-changed=proto/");

    Ok(())
}
