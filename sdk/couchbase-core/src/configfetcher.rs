/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::cbconfig;
use crate::configparser::ConfigParser;
use crate::configwatcher::ConfigWatcherMemd;
use crate::error::{Error, ErrorKind};
use crate::kv_orchestration::KvClientManagerClientType;
use crate::kvclient::{KvClient, StdKvClient};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientpool::KvClientPool;
use crate::kvendpointclientmanager::KvEndpointClientManager;
use crate::log_redaction::{not_redacted, system_data};
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::request::{GetClusterConfigKnownVersion, GetClusterConfigRequest};
use crate::parsedconfig::ParsedConfig;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout, timeout_at};
use tracing::{debug, trace};

// Dumps every fetched cluster config to the log, for debugging. The dump is not annotated for
// redaction, and creating an agent warns when it is set while redaction is on.
pub(crate) const DEBUG_CONFIG_ENV_VAR: &str = "RSCBC_DEBUG_CONFIG";

#[derive(Clone)]
pub(crate) struct ConfigFetcherMemd<M: KvEndpointClientManager> {
    kv_client_manager: Arc<M>,
    fetch_timeout: Duration,
}

pub(crate) struct ConfigFetcherMemdOptions<M: KvEndpointClientManager> {
    pub kv_client_manager: Arc<M>,
    pub fetch_timeout: Duration,
}

impl<M: KvEndpointClientManager> ConfigFetcherMemd<M> {
    pub fn new(opts: ConfigFetcherMemdOptions<M>) -> Self {
        Self {
            kv_client_manager: opts.kv_client_manager.clone(),
            fetch_timeout: opts.fetch_timeout,
        }
    }
    pub(crate) async fn poll_one(
        &self,
        endpoint: &str,
        rev_id: i64,
        rev_epoch: i64,
        skip_fetch_cb: impl FnOnce(Arc<KvClientManagerClientType<M>>) -> bool,
    ) -> crate::error::Result<Option<ParsedConfig>> {
        let client = self.kv_client_manager.get_endpoint_client(endpoint).await?;

        if skip_fetch_cb(client.clone()) {
            return Ok(None);
        }

        debug!("Fetching config from {}", system_data(endpoint));

        let hostname = client.remote_hostname();
        let known_version = {
            if rev_id > 0 && client.has_feature(HelloFeature::ClusterMapKnownVersion) {
                Some(GetClusterConfigKnownVersion { rev_epoch, rev_id })
            } else {
                None
            }
        };

        let resp = timeout(
            self.fetch_timeout,
            client.get_cluster_config(GetClusterConfigRequest { known_version }),
        )
        .await
        .map_err(|e| Error::new_message_error("get cluster config timed out"))?
        .map_err(Error::new_contextual_memdx_error)?;

        if resp.config.is_empty() {
            return Ok(None);
        }

        let config = cbconfig::parse::parse_terse_config(&resp.config, hostname)?;

        if env::var(DEBUG_CONFIG_ENV_VAR).is_ok() {
            // Not annotated: the dump exists to show exactly what the server sent, and is an
            // opt-in debugging aid rather than something a deployment leaves on.
            trace!("Fetcher fetched new config {:?}", not_redacted(&config));
        }

        Ok(Some(ConfigParser::parse_terse_config(config, hostname)?))
    }
}
