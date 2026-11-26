use crate::cluster::options::TimeoutOptions;
use crate::commands::sdk::run_with_timeout;
use crate::errors::error::Error;
use couchbase::cluster::Cluster;
use couchbase::conversational_query::ConversationalQueryChat;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::Status;
use tracing::dispatcher::Dispatch;

pub struct ConnectionSet {
    pub cluster: Cluster,
    pub tracing_subscriber: Dispatch,
    pub timeout_options: TimeoutOptions,
    // Conversational Query chats created by begin_chat and attach_chat, by the driver's chat_ref.
    pub chats: Mutex<HashMap<String, Arc<ConversationalQueryChat>>>,
}

impl ConnectionSet {
    pub async fn new(
        connstr: impl Into<String>,
        opts: crate::cluster::options::ClusterOptions,
        tracing_subscriber: Dispatch,
    ) -> Result<Self, Box<Error>> {
        let crate::cluster::options::ClusterOptions {
            cluster_options,
            timeout_options,
        } = opts;

        let cluster = run_with_timeout(
            Duration::from_secs(30),
            Self::connect(connstr, cluster_options),
        )
        .await?;

        Ok(Self {
            cluster,
            tracing_subscriber,
            timeout_options,
            chats: Mutex::new(HashMap::new()),
        })
    }

    async fn connect(
        conn_str: impl Into<String>,
        cluster_options: couchbase::options::cluster_options::ClusterOptions,
    ) -> Result<Cluster, Box<Error>> {
        Cluster::connect(conn_str.into(), cluster_options)
            .await
            .map_err(|e| {
                Box::new(Error::Status(Status::unknown(format!(
                    "failed to connect to cluster: {}",
                    e
                ))))
            })
    }
}
