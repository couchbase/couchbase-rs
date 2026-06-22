# Couchbase Rust FIT Performer

This is the FIT performer for the [Couchbase Rust SDK](https://github.com/couchbaselabs/couchbase-rs)

## Prerequisites

- Rust (latest stable recommended)
- Cargo
- Protocol buffer compiler (protoc)

## Building

The project uses protocol buffer definitions located in the `performer/proto/` directory.
These are automatically compiled during the build process.

```bash
cargo build -p fit-performer
```

If you encounter issues with proto file generation, try cleaning the build first:

```bash
cargo clean -p fit-performer
cargo build -p fit-performer
```

## Updating Proto Files

Run the update script from anywhere in the repository:

```bash
./performer/scripts/update-protobuf.sh
```

## Running
Once built, the performer can be run with:
```bash
cargo run -p fit-performer
```

## Docker

Build and run the performer image from the repository root:
```bash
docker build -f performer/Dockerfile -t fit-performer .
docker run -p 8060:8060 fit-performer
```


