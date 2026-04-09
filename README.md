# Couchbase Rust SDK

[![license](https://img.shields.io/github/license/couchbase/couchbase-jvm-clients?color=brightgreen)](https://opensource.org/licenses/Apache-2.0)

This repository contains the Couchbase Rust SDK.

## Project Structure

This repository contains multiple crates that work together to provide the complete Couchbase Rust SDK:

- **`couchbase`** - Main SDK crate providing the, public, high-level API.
- **`couchbase-core`** - Core networking and protocol implementation, not intended for direct use.
- **`couchbase-connstr`** - Connection string parsing and DNS resolution, not intended for direct use.
- **`protostellar`** - Couchbase2 protocol support, not intended for direct use.

## Quick Start

Add the Couchbase SDK to your `Cargo.toml`:

```
cargo add couchbase
```

## Building from Source

```
git clone https://github.com/couchbaselabs/couchbase-rs
cd sdk/couchbase
cargo build 
```

## Testing

Tests use the standard Rust testing framework. To run tests:

```
cd sdk/couchbase
export RCBCONNSTR="couchbases://127.0.0.1
export RCBCUSERNAME="username"
export RCBCPASSWORD="password"
cargo test
```

For a full list of available environment variables, see the [Testing Environment Variables](https://github.com/couchbaselabs/couchbase-rs/blob/main/sdk/couchbase/tests/common/test_config.rs).

### Test coverage

Whilst some integration tests are included in the main SDK crate, more extensive tests are run via the Couchbase FIT tool.

### Benchmarks

```bash
# Run benchmarks
cargo bench

# Run specific benchmark
cargo bench collection_crud
```

Benchmarks share the same environment variables as tests.

## Branching Strategy

The rust SDK follows the [semantic versioning](https://semver.org/) strategy.
Dotpatch releases contain only bug fixes.
Dotminor releases may contain new features, but are backwards compatible.
Dotmajor releases are very, very infrequent.

The `main` branch is where development for the next minor release happens.
For each new change a new branch is created and then the change is merged back into `main` via a pull request.

Maintenance branches are named `x.y` where `x` is the major version and `y` is the minor version.
Instead of committing directly to a maintenance branch, first commit to `main` and then cherry-pick to the maintenance branch if possible.

## Documentation

[Couchbase documentation site](https://docs.couchbase.com/rust-sdk/current/hello-world/overview.html)
[API reference documentation](https://docs.rs/couchbase/latest/couchbase/)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Releases

Crates are published automatically via the publish workflow when a tag is pushed to the repository.
At present all crates maintain the same version number, but in the future this may change.
This means that crates are version bumped even if they have had no changes, this is a trade-off to avoid the complexity of managing separate versions for each crate.
