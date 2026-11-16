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

use std::time::{Duration, Instant};

use couchbase_core::address::Address;
use couchbase_core::agent::Agent;
use couchbase_core::authenticator::{Authenticator, PasswordAuthenticator};
use couchbase_core::error::ErrorKind;
use couchbase_core::options::agent::SeedConfig;

use crate::common::default_agent_options::create_default_options;
use crate::common::test_config::setup_test;

mod common;

#[test]
fn test_bootstrap_wrong_password_fails_fast() {
    setup_test(async |config| {
        let username = config.username.clone();
        let opts = create_default_options(config)
            .await
            .authenticator(Authenticator::PasswordAuthenticator(
                PasswordAuthenticator {
                    username,
                    password: "wrong-password".to_string(),
                },
            ))
            .bootstrap_timeout(Duration::from_secs(30));

        let start = Instant::now();
        let err = Agent::new(opts).await.err().expect("bootstrap should fail");

        assert!(start.elapsed() < Duration::from_secs(10));
        let ErrorKind::Bootstrap(e) = err.kind() else {
            panic!("expected a bootstrap error, got {err}");
        };
        assert!(!e.timed_out());
        assert!(e.is_authentication_failure(), "{e}");
    });
}

#[test]
fn test_bootstrap_unreachable_seed_times_out() {
    setup_test(async |config| {
        let opts = create_default_options(config)
            .await
            .seed_config(SeedConfig::new().memd_addrs(vec![Address {
                host: "127.0.0.1".to_string(),
                port: 1,
            }]))
            .bootstrap_timeout(Duration::from_secs(2));

        let start = Instant::now();
        let err = Agent::new(opts).await.err().expect("bootstrap should fail");

        assert!(start.elapsed() < Duration::from_secs(5));
        let ErrorKind::Bootstrap(e) = err.kind() else {
            panic!("expected a bootstrap error, got {err}");
        };
        assert!(e.timed_out());
        assert!(!e.is_authentication_failure());
        assert_eq!(e.seed_errors().len(), 1);
        assert_eq!(e.seed_errors()[0].seed(), "127.0.0.1:1");
    });
}
