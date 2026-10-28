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

use std::future::Future;
use std::ops::Deref;
use std::sync::Weak;

use tokio::sync::Mutex;

pub(crate) trait NotMyVbucketConfigHandler: Send + Sync {
    // The config is passed unparsed, as $HOST must be substituted for the hostname of
    // the client that the config came from.
    fn not_my_vbucket_config(
        &self,
        config: &[u8],
        source_hostname: &str,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) trait ConfigUpdater: Send + Sync + Sized {
    fn apply_terse_config(
        &self,
        config: &[u8],
        source_hostname: &str,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) struct StdNotMyVbucketConfigHandler<C> {
    watcher: Mutex<Option<Weak<C>>>,
}

impl<C> StdNotMyVbucketConfigHandler<C>
where
    C: ConfigUpdater,
{
    pub fn new() -> Self {
        Self {
            watcher: Mutex::new(None),
        }
    }

    pub async fn set_watcher(&self, updater: Weak<C>) {
        let mut watcher = self.watcher.lock().await;
        *watcher = Some(updater);
    }
}

impl<C> NotMyVbucketConfigHandler for StdNotMyVbucketConfigHandler<C>
where
    C: ConfigUpdater,
{
    async fn not_my_vbucket_config(&self, config: &[u8], source_hostname: &str) {
        if let Some(watcher) = self.watcher.lock().await.deref() {
            if let Some(watcher) = watcher.upgrade() {
                watcher.apply_terse_config(config, source_hostname).await;
            }
        }
    }
}
