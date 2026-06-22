use crate::cluster::connection::ConnectionSet;
use crate::commands::sdk::build_sdk_command;
use crate::common::batcher::Batcher;
use crate::counters::counter::Counters;
use crate::errors::error::{Error, Result};
use crate::observability::span_owner::SpanOwner;
use crate::proto::protocol::sdk;
use crate::streams::stream_owner::StreamOwner;
use std::sync::Arc;

pub struct Executor {
    conn: Arc<ConnectionSet>,
    counters: Arc<Counters>,
    stream_owner: Arc<StreamOwner>,
    span_owner: Arc<SpanOwner>,
    run_id: String,
}

impl Executor {
    pub fn new(
        conn: Arc<ConnectionSet>,
        counters: Arc<Counters>,
        stream_owner: Arc<StreamOwner>,
        span_owner: Arc<SpanOwner>,
        run_id: String,
    ) -> Self {
        Self {
            conn,
            counters,
            stream_owner,
            span_owner,
            run_id,
        }
    }

    pub async fn perform_operation(
        &self,
        command: sdk::Command,
        batcher: &Batcher,
    ) -> Result<bool> {
        match build_sdk_command(
            self.conn.clone(),
            command,
            self.counters.clone(),
            self.span_owner.clone(),
        ) {
            Ok(cmd) => {
                cmd.execute(batcher, &self.stream_owner, self.run_id.clone())
                    .await
            }
            Err(e) => match *e {
                Error::Sdk(e) => {
                    batcher.send(*e).await;
                    Ok(false)
                }
                _ => Err(e),
            },
        }
    }
}
