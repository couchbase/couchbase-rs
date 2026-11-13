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

use crate::common::default_agent_options::create_default_options;
use crate::common::helpers::{generate_bytes_value, generate_key};
use crate::common::test_config::setup_test;
use couchbase_core::agent::Agent;
use couchbase_core::memdx::magic::Magic;
use couchbase_core::memdx::opcode::OpCode;
use couchbase_core::memdx::packet::ResponsePacket;
use couchbase_core::options::crud::UpsertOptions;
use couchbase_core::options::orphan_reporter::OrphanReporterConfig;
use couchbase_core::options::waituntilready::WaitUntilReadyOptions;
use couchbase_core::orphan_reporter::{OrphanContext, OrphanReporter};
use couchbase_core::retryfailfast::FailFastRetryStrategy;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

mod common;

fn make_server_duration_frame(micros: u16) -> Vec<u8> {
    // Build a single server-duration response frame:
    // header: high nibble = code (0x0 => 0 -> ServerDuration), low nibble = len (2)
    // body: 2 bytes encoded duration (we'll just pass through micros here for testing)
    let mut buf = Vec::with_capacity(1 + 2);
    let frame_header: u8 = 0x02; // code=0x00, len=2
    buf.push(frame_header);
    buf.push((micros >> 8) as u8);
    buf.push((micros & 0xff) as u8);
    buf
}

#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn captured_lines(writer: &CaptureWriter) -> Vec<String> {
    let bytes = writer.0.lock().unwrap().clone();
    let s = String::from_utf8(bytes.clone())
        .unwrap_or_else(|_| panic!("captured output is not valid UTF-8: {:?}", bytes));
    s.lines().map(|l| l.to_string()).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn orphan_reporter_task_stops_when_dropped() {
    let alive_tasks = || {
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
    };
    let baseline = alive_tasks();

    let reporter = OrphanReporter::new(
        OrphanReporterConfig::default().reporter_interval(Duration::from_secs(3600)),
    );
    let handle = reporter.get_handle();
    tokio::task::yield_now().await;
    assert_eq!(alive_tasks(), baseline + 1);

    // The task must outlive the reporter while a handle is still in use.
    drop(reporter);
    tokio::task::yield_now().await;
    assert_eq!(alive_tasks(), baseline + 1);

    drop(handle);
    tokio::time::timeout(Duration::from_secs(5), async {
        while alive_tasks() > baseline {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("orphan reporter task should stop once the reporter and its handles are dropped");
}

#[tokio::test(flavor = "current_thread")]
async fn orphan_reporter_emits_entries() {
    let writer = CaptureWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();

    let _guard = tracing::subscriber::set_default(subscriber);

    let cfg = OrphanReporterConfig::default()
        .reporter_interval(Duration::from_millis(500))
        .sample_size(5);

    let reporter = OrphanReporter::new(cfg);
    let handle = reporter.get_handle();
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10000);
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 11210);
    let ctx = OrphanContext {
        client_id: "test-client".to_string(),
        local_addr: local,
        peer_addr: peer,
    };

    // Build a few ResponsePackets with flexible (extended) magic and framing extras
    for (i, micros) in [300u16, 1200, 50, 8000, 400].iter().enumerate() {
        let mut pkt = ResponsePacket::new(
            Magic::ResExt,
            OpCode::Get,
            0,
            couchbase_core::memdx::status::Status::Success,
            i as u32,
        );
        pkt.framing_extras = Some(make_server_duration_frame(*micros).into());
        handle(pkt, ctx.clone());
    }

    // Allow time for background task to flush
    tokio::time::sleep(Duration::from_secs(2)).await;

    let lines = captured_lines(&writer);
    let log_out = lines
        .iter()
        .find(|l| l.contains("Orphaned responses observed:"))
        .expect("expected a log line containing 'Orphaned responses observed:'");

    let prefix = "Orphaned responses observed: ";
    let json_str = log_out
        .split_once(prefix)
        .map(|x| x.1)
        .expect("expected JSON payload after prefix in log line");

    // Parse JSON
    let v: serde_json::Value = serde_json::from_str(json_str).expect("valid JSON expected");

    // Expect service map with key "kv"
    let obj = v.as_object().expect("service map object expected");
    let (svc_key, entry_val) = obj
        .get_key_value("kv")
        .or_else(|| obj.iter().next())
        .expect("service map non-empty");
    assert_eq!(svc_key, "kv");

    // Entry shape
    let total_count = entry_val
        .get("total_count")
        .and_then(|x| x.as_u64())
        .expect("total_count u64");
    assert_eq!(total_count, 5, "expected total_count 5");

    let top = entry_val
        .get("top_requests")
        .and_then(|x| x.as_array())
        .expect("top_requests array");
    assert_eq!(top.len(), 5, "expected 5 top items");

    // Validate ordering and exact field values we set
    // Inputs were micros: [300, 1200, 50, 8000, 400] => order by duration desc: indices [3,1,4,0,2]
    let expected_op_ids = ["0x3", "0x1", "0x4", "0x0", "0x2"]; // packet opaque
    for (idx, item) in top.iter().enumerate() {
        // Required string fields
        let op_name = item
            .get("operation_name")
            .and_then(|x| x.as_str())
            .expect("operation_name");
        assert_eq!(op_name, "Get");

        let last_local_id = item
            .get("last_local_id")
            .and_then(|x| x.as_str())
            .expect("last_local_id");
        assert_eq!(last_local_id, "test-client");

        let op_id = item
            .get("operation_id")
            .and_then(|x| x.as_str())
            .expect("operation_id");
        assert_eq!(op_id, expected_op_ids[idx]);

        let last_local_socket = item
            .get("last_local_socket")
            .and_then(|x| x.as_str())
            .expect("last_local_socket");
        assert!(last_local_socket.contains("127.0.0.1:10000"));

        let last_remote_socket = item
            .get("last_remote_socket")
            .and_then(|x| x.as_str())
            .expect("last_remote_socket");
        assert!(last_remote_socket.contains("10.0.0.1:11210"));

        // Duration fields present
        for key in ["last_server_duration_us", "total_server_duration_us"] {
            let n = item.get(key).and_then(|x| x.as_u64());
            assert!(n.is_some(), "{} missing/u64", key);
        }
    }

    // Validate non-increasing order by total_server_duration_us
    let mut prev: Option<u64> = None;
    for (idx, it) in top.iter().enumerate() {
        let v = it
            .get("total_server_duration_us")
            .and_then(|x| x.as_u64())
            .expect("total_server_duration_us u64");
        if let Some(p) = prev {
            assert!(
                p >= v,
                "items not sorted descending by total_server_duration_us: {} < {} at index {}",
                p,
                v,
                idx
            );
        }
        prev = Some(v);
    }
}

#[test]
fn test_orphan_reporter_logs() {
    setup_test(async |config| {
        let writer = CaptureWriter::default();
        let capture_layer = tracing_subscriber::fmt::layer()
            .with_writer(writer.clone())
            .with_ansi(false);

        let agent_opts = create_default_options(config)
            .await
            .orphan_reporter_handler(
                OrphanReporter::new(
                    OrphanReporterConfig::default()
                        .reporter_interval(Duration::from_secs(1))
                        .sample_size(2),
                )
                .get_handle(),
            );

        // Install the capture layer for the duration of this test
        let subscriber = tracing_subscriber::registry().with(capture_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        let agent = Agent::new(agent_opts).await.unwrap();
        agent
            .wait_until_ready(&WaitUntilReadyOptions::new())
            .await
            .unwrap();

        let strat = Arc::new(FailFastRetryStrategy::default());
        let value = generate_bytes_value(32);

        // Run ops until we see a timeout (which should be an orphan)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut got_timeout = false;
        while tokio::time::Instant::now() < deadline && !got_timeout {
            for _ in 0..10u8 {
                let key = generate_key();
                let res = tokio::time::timeout(
                    Duration::from_nanos(1),
                    agent.upsert(
                        UpsertOptions::new(key.as_slice(), "", "", value.as_slice())
                            .retry_strategy(strat.clone()),
                    ),
                )
                .await;
                if res.is_err() {
                    got_timeout = true;
                }
            }
            if !got_timeout {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        assert!(
            got_timeout,
            "expected at least one timeout to generate orphans"
        );

        // Allow reporter to flush
        tokio::time::sleep(Duration::from_secs(2)).await;

        let lines = captured_lines(&writer);
        let log_out = lines
            .iter()
            .find(|l| l.contains("Orphaned responses observed:"))
            .expect("expected a log line containing 'Orphaned responses observed:'");

        let prefix = "Orphaned responses observed: ";
        let json_str = log_out
            .split_once(prefix)
            .map(|x| x.1)
            .expect("expected JSON payload after prefix in log line");

        let v: serde_json::Value = serde_json::from_str(json_str).expect("valid JSON expected");

        let is_entry_level = v.get("top_requests").is_some();
        let (entry, service_key) = if is_entry_level {
            (v, None::<&String>)
        } else {
            let obj = v.as_object().expect("service map object expected");
            let kv_or_first = obj.get_key_value("kv").or_else(|| obj.iter().next());
            assert!(kv_or_first.is_some(), "service map non-empty expected");
            let (k, e) = kv_or_first.unwrap();
            (e.clone(), Some(k))
        };

        // Validate entry shape
        let total_count = entry
            .get("total_count")
            .and_then(|x| x.as_u64())
            .expect("total_count missing");
        assert!(total_count >= 1, "total_count should be >= 1");

        let top = entry
            .get("top_requests")
            .and_then(|x| x.as_array())
            .expect("top_requests missing");
        assert!(!top.is_empty(), "top_requests is empty");

        // Validate sorting: total_server_duration_us must be non-increasing
        let mut prev: Option<u64> = None;
        for (idx, it) in top.iter().enumerate() {
            let v = it
                .get("total_server_duration_us")
                .and_then(|x| x.as_u64())
                .unwrap_or_else(|| panic!("top[{}].total_server_duration_us missing", idx));
            if let Some(p) = prev {
                assert!(
                    p >= v,
                    "items not sorted descending by total_server_duration_us: {} < {} at index {}",
                    p,
                    v,
                    idx
                );
            }
            prev = Some(v);
        }

        // Check all fields are present
        let item = &top[0];
        for key in ["last_server_duration_us", "total_server_duration_us"] {
            let n = item.get(key).and_then(|x| x.as_u64());
            assert!(n.is_some(), "{} missing/u64", key);
        }
        for key in [
            "operation_name",
            "last_local_id",
            "operation_id",
            "last_local_socket",
            "last_remote_socket",
        ] {
            let s = item.get(key).and_then(|x| x.as_str());
            assert!(s.is_some(), "{} missing/str", key);
            assert!(!s.unwrap().is_empty(), "{} must be non-empty", key);
        }

        if let Some(svc) = service_key {
            assert_eq!(svc, "kv", "expected kv service key");
        }
    });
}
