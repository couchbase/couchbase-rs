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

use crate::log_redaction::system_data;
use crate::memdx::extframe::decode_res_ext_frames;
use crate::memdx::packet::ResponsePacket;
use crate::options::orphan_reporter::OrphanReporterConfig;
use serde_json::json;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::mem;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::{interval_at, Instant, MissedTickBehavior};
use tracing::{trace, warn};

#[derive(Debug, Clone)]
pub struct OrphanContext {
    pub client_id: String,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

#[derive(Debug, Eq)]
struct OrphanLogItem {
    pub connection_id: String,
    pub operation_id: String,
    pub remote_socket: String,
    pub local_socket: String,
    pub server_duration: Duration,
    pub total_server_duration: Duration,
    pub operation_name: String,
}

impl Display for OrphanLogItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let obj = json!({
            "last_server_duration_us": self.server_duration.as_micros() as u64,
            "total_server_duration_us": self.total_server_duration.as_micros() as u64,
            "operation_name": self.operation_name,
            "last_local_id": self.connection_id,
            "operation_id": self.operation_id,
            "last_local_socket": self.local_socket,
            "last_remote_socket": self.remote_socket,
        });
        write!(f, "{}", obj)
    }
}

struct OrphanLogJsonEntry {
    count: u64,
    top_items: Vec<OrphanLogItem>,
}

impl Display for OrphanLogJsonEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bodies: Vec<String> = self.top_items.iter().map(|item| item.to_string()).collect();
        write!(
            f,
            r#"{{"total_count":{},"top_requests":[{}]}}"#,
            self.count,
            bodies.join(",")
        )
    }
}

struct OrphanLogService(HashMap<String, OrphanLogJsonEntry>);

impl Display for OrphanLogService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        for (svc, entry) in &self.0 {
            parts.push(format!(r#""{}":{}"#, svc, entry));
        }
        write!(f, "{{{}}}", parts.join(","))
    }
}

// Once we have total_duration added we will order on that
impl PartialEq for OrphanLogItem {
    fn eq(&self, other: &Self) -> bool {
        self.total_server_duration == other.total_server_duration
    }
}

impl Ord for OrphanLogItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_server_duration.cmp(&other.total_server_duration)
    }
}

impl PartialOrd for OrphanLogItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Orphan reporter - Currently only handles/receives KV orphans
pub struct OrphanReporter {
    total_count: Arc<AtomicU64>,
    heap: Arc<RwLock<BinaryHeap<Reverse<OrphanLogItem>>>>,
    sample_size: usize,
    reporter_interval: Duration,
}

impl OrphanReporter {
    pub fn new(config: OrphanReporterConfig) -> Self {
        let heap = Arc::new(RwLock::new(BinaryHeap::with_capacity(config.sample_size)));
        let total_count = Arc::new(AtomicU64::new(0));

        let heap_clone = heap.clone();
        let total_count_clone = total_count.clone();

        tokio::spawn(async move {
            trace!(
                "OrphanReporter started: reporter_interval={:?}, sample_size={}",
                config.reporter_interval,
                config.sample_size
            );
            let start = Instant::now() + config.reporter_interval;
            let mut tick = interval_at(start, config.reporter_interval);
            tick.set_missed_tick_behavior(MissedTickBehavior::Burst);

            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let count = total_count_clone.swap(0, Ordering::Relaxed);
                        if count == 0 {
                            continue;
                        }
                        let mut write_guard = heap_clone.write().unwrap();
                        let obj = Self::create_log_object("kv".to_string(), mem::take(&mut write_guard), count);
                        // Tagged as one span around the whole report rather than per value,
                        // so that the report stays parseable JSON. It carries local and remote
                        // sockets, and otherwise only durations and SDK-generated ids, so it is
                        // system data.
                        warn!("Orphaned responses observed: {}", system_data(&obj));
                    }
                }
            }
        });
        Self {
            total_count,
            heap,
            sample_size: config.sample_size,
            reporter_interval: config.reporter_interval,
        }
    }

    pub fn get_handle(&self) -> Arc<dyn Fn(ResponsePacket, OrphanContext) + Send + Sync> {
        let heap = self.heap.clone();
        let total_count = self.total_count.clone();
        let sample_size = self.sample_size;

        Arc::new(move |msg: ResponsePacket, ctx: OrphanContext| {
            total_count.fetch_add(1, Ordering::Relaxed);

            let server_dur = msg
                .framing_extras
                .as_deref()
                .and_then(|f| decode_res_ext_frames(f).ok().flatten())
                .unwrap_or_default();

            // Read-only
            let (current_length, current_min) = {
                let guard = heap.read().unwrap_or_else(|p| {
                    warn!("OrphanReporter heap poisoned; continuing");
                    p.into_inner()
                });
                (
                    guard.len(),
                    guard.peek().map(|Reverse(i)| i.total_server_duration),
                )
            };

            let needs_write = current_length < sample_size
                || current_min.map(|m| server_dur > m).unwrap_or(false);

            if needs_write {
                let mut write_guard = heap.write().unwrap_or_else(|p| {
                    warn!("OrphanReporter heap poisoned; continuing");
                    p.into_inner()
                });

                if write_guard.len() < sample_size {
                    write_guard.push(Reverse(OrphanLogItem {
                        connection_id: ctx.client_id,
                        operation_id: format!("0x{:x}", msg.opaque),
                        remote_socket: ctx.peer_addr.to_string(),
                        local_socket: ctx.local_addr.to_string(),
                        server_duration: server_dur,
                        total_server_duration: server_dur,
                        operation_name: format!("{:?}", msg.op_code),
                    }));
                } else if let Some(Reverse(min)) = write_guard.peek() {
                    if server_dur > min.total_server_duration {
                        write_guard.pop();
                        write_guard.push(Reverse(OrphanLogItem {
                            connection_id: ctx.client_id,
                            operation_id: format!("0x{:x}", msg.opaque),
                            remote_socket: ctx.peer_addr.to_string(),
                            local_socket: ctx.local_addr.to_string(),
                            server_duration: server_dur,
                            total_server_duration: server_dur,
                            operation_name: format!("{:?}", msg.op_code),
                        }));
                    }
                }
            }
        })
    }

    fn create_log_object(
        service: String,
        heap_items: BinaryHeap<Reverse<OrphanLogItem>>,
        count: u64,
    ) -> OrphanLogService {
        let items: Vec<OrphanLogItem> = heap_items
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(item)| item)
            .collect();
        let entry = OrphanLogJsonEntry {
            count,
            top_items: items,
        };
        let mut services = HashMap::new();
        services.insert(service, entry);
        OrphanLogService(services)
    }
}
