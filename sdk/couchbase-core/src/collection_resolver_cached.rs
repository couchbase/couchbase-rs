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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use tokio::sync::futures::OwnedNotified;
use tokio::sync::Notify;

use crate::collectionresolver::CollectionResolver;
use crate::error::Error;
use crate::error::Result;

struct CollectionsFastCacheEntry {
    pub collection_id: u32,
    pub manifest_rev: u64,
}

#[derive(Default)]
struct CollectionsFastManifest {
    pub collections: HashMap<String, CollectionsFastCacheEntry>,
}

#[derive(Clone)]
struct CollectionCacheEntry {
    // TODO: Strongly suspect these should be Option.
    collection_id: u32,
    manifest_rev: u64,

    pending: Option<Arc<Notify>>,
}

type CollectionResolverSlowMap = Arc<Mutex<HashMap<String, Arc<Mutex<CollectionCacheEntry>>>>>;

enum SlowMapLookup {
    Pending(OwnedNotified),
    Inserted(Arc<Mutex<CollectionCacheEntry>>),
}

pub(crate) struct CollectionResolverCached<Resolver: CollectionResolver> {
    resolver: Arc<Resolver>,

    fast_cache: Arc<ArcSwap<CollectionsFastManifest>>,

    slow_map: CollectionResolverSlowMap,
}

#[derive(Clone)]
pub(crate) struct CollectionResolverCachedOptions<Resolver: CollectionResolver> {
    pub resolver: Resolver,
}

impl<Resolver> CollectionResolverCached<Resolver>
where
    Resolver: CollectionResolver + 'static,
{
    pub fn new(opts: CollectionResolverCachedOptions<Resolver>) -> Self {
        Self {
            resolver: Arc::new(opts.resolver),
            fast_cache: Arc::new(ArcSwap::from_pointee(Default::default())),
            slow_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn resolve_collection_id_slow(
        &self,
        scope_name: &str,
        collection_name: &str,
        full_key_path: &str,
    ) -> Result<(u32, u64)> {
        loop {
            // Scoped so the compiler sees the slow_map lock released before we await.
            let lookup = {
                let mut slow_map = self.slow_map.lock().unwrap();

                if let Some(entry) = slow_map.get(full_key_path) {
                    let entry_guard = entry.lock().unwrap();
                    if let Some(pending) = &entry_guard.pending {
                        // Created under the lock, notify_waiters only wakes existing futures.
                        SlowMapLookup::Pending(pending.clone().notified_owned())
                    } else {
                        return Ok((entry_guard.collection_id, entry_guard.manifest_rev));
                    }
                } else {
                    let entry = Arc::new(Mutex::new(CollectionCacheEntry {
                        collection_id: 0,
                        manifest_rev: 0,
                        pending: Some(Arc::new(Notify::new())),
                    }));

                    slow_map.insert(full_key_path.to_string(), entry.clone());

                    SlowMapLookup::Inserted(entry)
                }
            };

            // Keep our own entry rather than looking it up again later, it may have been
            // invalidated (and replaced) whilst the resolve was in flight.
            let entry = match lookup {
                SlowMapLookup::Pending(notified) => {
                    notified.await;

                    continue;
                }
                SlowMapLookup::Inserted(entry) => entry,
            };

            let resolve_resp = {
                let scope_name = scope_name.to_string();
                let collection_name = collection_name.to_string();
                let resolver = self.resolver.clone();
                let handle = tokio::spawn(async move {
                    resolver
                        .resolve_collection_id(&scope_name, &collection_name)
                        .await
                });

                handle.await
            }
            .map_err(|e| {
                Error::new_message_error(format!("failed to join resolve collection id task: {e}"))
            })?;

            return match resolve_resp {
                Ok((collection_id, manifest_rev)) => {
                    let slow_map = self.slow_map.lock().unwrap();

                    let pending = {
                        let mut guard = entry.lock().unwrap();
                        guard.collection_id = collection_id;
                        guard.manifest_rev = manifest_rev;

                        guard.pending.take()
                    };

                    if Self::is_current_entry_locked(&slow_map, full_key_path, &entry) {
                        Self::rebuild_fast_cache_locked(&slow_map, self.fast_cache.clone());
                    }

                    if let Some(p) = pending {
                        p.notify_waiters();
                    }

                    Ok((collection_id, manifest_rev))
                }
                Err(e) => {
                    let mut slow_map = self.slow_map.lock().unwrap();
                    if Self::is_current_entry_locked(&slow_map, full_key_path, &entry) {
                        slow_map.remove(full_key_path);
                    }

                    let mut guard = entry.lock().unwrap();
                    let pending = guard.pending.take();

                    // No need to rebuild the fast cache as we haven't added this entry to it.

                    if let Some(p) = pending {
                        p.notify_waiters();
                    }

                    Err(e)
                }
            };
        }
    }

    fn is_current_entry_locked(
        guard: &HashMap<String, Arc<Mutex<CollectionCacheEntry>>>,
        full_key_path: &str,
        entry: &Arc<Mutex<CollectionCacheEntry>>,
    ) -> bool {
        guard
            .get(full_key_path)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
    }

    fn rebuild_fast_cache_locked(
        guard: &HashMap<String, Arc<Mutex<CollectionCacheEntry>>>,
        fast_cache: Arc<ArcSwap<CollectionsFastManifest>>,
    ) {
        let mut collections = HashMap::new();
        for (full_key_path, entry) in guard.iter() {
            let (collection_id, manifest_rev) = {
                let guard = entry.lock().unwrap();
                (guard.collection_id, guard.manifest_rev)
            };

            if collection_id > 0 {
                collections.insert(
                    full_key_path.clone(),
                    CollectionsFastCacheEntry {
                        collection_id,
                        manifest_rev,
                    },
                );
            }
        }

        fast_cache.store(Arc::new(CollectionsFastManifest { collections }));
    }
}

impl<Resolver> CollectionResolver for CollectionResolverCached<Resolver>
where
    Resolver: CollectionResolver + 'static,
{
    async fn resolve_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
    ) -> Result<(u32, u64)> {
        let full_key_path = scope_name.to_owned() + "." + collection_name;

        {
            let fast_cache = self.fast_cache.load();
            if let Some(entry) = fast_cache.collections.get(&full_key_path) {
                return Ok((entry.collection_id, entry.manifest_rev));
            }
        }

        self.resolve_collection_id_slow(scope_name, collection_name, &full_key_path)
            .await
    }

    async fn invalidate_collection_id(&self, scope_name: &str, collection_name: &str) {
        self.resolver
            .invalidate_collection_id(scope_name, collection_name)
            .await;

        let full_key_path = scope_name.to_owned() + "." + collection_name;

        let mut slow_map = self.slow_map.lock().unwrap();
        slow_map.remove(&full_key_path);

        Self::rebuild_fast_cache_locked(&slow_map, self.fast_cache.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use tokio::sync::Notify;

    use crate::collection_resolver_cached::{
        CollectionResolverCached, CollectionResolverCachedOptions,
    };
    use crate::collectionresolver::CollectionResolver;
    use crate::error::{Error, Result};

    #[derive(Default)]
    struct GatedResolver {
        started: Notify,
        release: Notify,
        calls: AtomicUsize,
        fail: AtomicBool,
    }

    impl CollectionResolver for Arc<GatedResolver> {
        async fn resolve_collection_id(
            &self,
            _scope_name: &str,
            _collection_name: &str,
        ) -> Result<(u32, u64)> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;

            if self.fail.load(Ordering::SeqCst) {
                Err(Error::new_message_error("resolve failed"))
            } else {
                Ok((8, 1))
            }
        }

        async fn invalidate_collection_id(&self, _scope_name: &str, _collection_name: &str) {}
    }

    async fn resolve_with_invalidate_in_flight(
        fail: bool,
    ) -> (
        Arc<GatedResolver>,
        Arc<CollectionResolverCached<Arc<GatedResolver>>>,
    ) {
        let inner = Arc::new(GatedResolver::default());
        inner.fail.store(fail, Ordering::SeqCst);

        let resolver = Arc::new(CollectionResolverCached::new(
            CollectionResolverCachedOptions {
                resolver: inner.clone(),
            },
        ));

        let resolve_handle = {
            let resolver = resolver.clone();
            tokio::spawn(async move { resolver.resolve_collection_id("scope", "coll").await })
        };

        inner.started.notified().await;
        resolver.invalidate_collection_id("scope", "coll").await;
        inner.release.notify_one();

        let res = resolve_handle.await.expect("resolve task panicked");
        if fail {
            assert!(res.is_err());
        } else {
            assert_eq!((8, 1), res.unwrap());
        }

        (inner, resolver)
    }

    #[tokio::test]
    async fn invalidate_during_successful_resolve() {
        let (inner, resolver) = resolve_with_invalidate_in_flight(false).await;

        // The invalidated result must not have been cached, so this resolves again.
        inner.release.notify_one();
        let res = resolver.resolve_collection_id("scope", "coll").await;
        assert_eq!((8, 1), res.unwrap());
        assert_eq!(2, inner.calls.load(Ordering::SeqCst));

        // That result was not invalidated, so it is now cached.
        let res = resolver.resolve_collection_id("scope", "coll").await;
        assert_eq!((8, 1), res.unwrap());
        assert_eq!(2, inner.calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn invalidate_during_failed_resolve() {
        let (inner, resolver) = resolve_with_invalidate_in_flight(true).await;

        inner.fail.store(false, Ordering::SeqCst);
        inner.release.notify_one();
        let res = resolver.resolve_collection_id("scope", "coll").await;
        assert_eq!((8, 1), res.unwrap());
        assert_eq!(2, inner.calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn failed_resolve_does_not_remove_replacement_entry() {
        let inner = Arc::new(GatedResolver::default());
        let resolver = Arc::new(CollectionResolverCached::new(
            CollectionResolverCachedOptions {
                resolver: inner.clone(),
            },
        ));

        let spawn_resolve = || {
            let resolver = resolver.clone();
            tokio::spawn(async move { resolver.resolve_collection_id("scope", "coll").await })
        };

        // Invalidate the first resolve, then start a second which inserts a replacement entry.
        let first_handle = spawn_resolve();
        inner.started.notified().await;
        resolver.invalidate_collection_id("scope", "coll").await;

        let second_handle = spawn_resolve();
        inner.started.notified().await;

        // Notify wakes waiters in FIFO order, so this releases the first resolve.
        inner.fail.store(true, Ordering::SeqCst);
        inner.release.notify_one();
        let res = first_handle.await.expect("first resolve task panicked");
        assert!(res.is_err());

        inner.fail.store(false, Ordering::SeqCst);
        inner.release.notify_one();
        let res = second_handle.await.expect("second resolve task panicked");
        assert_eq!((8, 1), res.unwrap());

        // Should be cached, release anyway so a re-resolve fails the test rather than hangs.
        inner.release.notify_one();
        let res = resolver.resolve_collection_id("scope", "coll").await;
        assert_eq!((8, 1), res.unwrap());
        assert_eq!(2, inner.calls.load(Ordering::SeqCst));
    }
}
