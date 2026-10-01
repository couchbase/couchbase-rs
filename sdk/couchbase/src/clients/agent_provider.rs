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

use crate::error;
use crate::error::ErrorKind;
use couchbase_core::agent::Agent;
use couchbase_core::log_redaction::{metadata, user_data};
use couchbase_core::ondemand_agentmanager::OnDemandAgentManager;
use std::sync::{Arc, RwLock, Weak};
use tokio::sync::Notify;

#[derive(Clone)]
pub(crate) struct CouchbaseAgentProvider {
    inner: Arc<CouchbaseAgentProviderInner>,
    agent_create_handle: Option<Arc<tokio::task::JoinHandle<()>>>,
}

struct CouchbaseAgentProviderInner {
    agent: RwLock<Option<Weak<Agent>>>,
    waiter: Notify,
}

impl CouchbaseAgentProvider {
    pub fn with_agent(agent: Weak<Agent>) -> Self {
        Self {
            inner: Arc::new(CouchbaseAgentProviderInner {
                agent: RwLock::new(Some(agent)),
                waiter: Notify::new(),
            }),
            agent_create_handle: None,
        }
    }

    pub fn with_bucket(agent_manager: Arc<OnDemandAgentManager>, bucket_name: String) -> Self {
        let inner = Arc::new(CouchbaseAgentProviderInner {
            agent: RwLock::new(None),
            waiter: Notify::new(),
        });

        let inner_clone = inner.clone();
        let handle = tokio::spawn(async move {
            loop {
                let agent = match agent_manager.get_bucket_agent(bucket_name.clone()).await {
                    Ok(agent) => agent,
                    Err(e) => {
                        tracing::error!(
                            "failed to get agent for bucket {}: {}",
                            metadata(&bucket_name),
                            user_data(&e)
                        );
                        continue;
                    }
                };
                {
                    let mut guard = inner_clone.agent.write().unwrap();
                    *guard = Some(agent);
                }
                inner_clone.waiter.notify_waiters();
                return;
            }
        });

        Self {
            inner,
            agent_create_handle: Some(Arc::new(handle)),
        }
    }

    // get_agent will return the agent if it is already available, otherwise it will wait until it is available.
    pub async fn get_agent(&self) -> Weak<Agent> {
        {
            let guard = self.inner.agent.read().unwrap();
            if let Some(agent) = guard.as_ref() {
                return agent.clone();
            }
        }

        self.inner.waiter.notified().await;
        Box::pin(self.get_agent()).await
    }

    pub(crate) fn upgrade_agent(agent: Weak<Agent>) -> error::Result<Arc<Agent>> {
        match agent.upgrade() {
            Some(agent) => Ok(agent),
            None => Err(error::Error::new(ErrorKind::ClusterDropped)),
        }
    }
}
