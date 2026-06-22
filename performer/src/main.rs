#![recursion_limit = "256"]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::large_enum_variant)]

use crate::performer::Performer;
use crate::proto::protocol::performer_service_server::PerformerServiceServer;
use log::{info, LevelFilter};
use std::env;
use std::io::Write;
use tonic::transport::Server;

mod cluster;
mod performer;

mod commands;
mod common;
mod counters;
mod errors;
mod observability;
#[allow(non_camel_case_types)]
#[allow(clippy::all, dead_code)]
#[rustfmt::skip]
mod proto;
mod streams;
mod translations;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = env::var("PORT").unwrap_or("8060".to_string());
    let address = format!("0.0.0.0:{port}").parse().unwrap();

    env_logger::Builder::new()
        .format(|buf, record| {
            let prefix = if record.target().starts_with("fit_performer") {
                "[RUST-PERFORMER] "
            } else {
                ""
            };
            writeln!(
                buf,
                "{} {}:{} {} [{}] - {}",
                prefix,
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                record.level(),
                record.args()
            )
        })
        .filter_level(LevelFilter::Warn)
        .filter(
            Some("couchbase"),
            env::var("LOG_LEVEL")
                .unwrap_or("INFO".to_string())
                .parse()
                .unwrap(),
        )
        .filter(Some("couchbase::tracing"), LevelFilter::Off)
        .filter(Some("couchbase::metrics"), LevelFilter::Off)
        .filter(
            Some("fit_performer"),
            env::var("LOG_LEVEL")
                .unwrap_or("INFO".to_string())
                .parse()
                .unwrap(),
        )
        .init();

    info!("Rust FIT performer service starting on {address}");

    Server::builder()
        .add_service(PerformerServiceServer::new(Performer::default()))
        .serve(address)
        .await?;

    Ok(())
}
