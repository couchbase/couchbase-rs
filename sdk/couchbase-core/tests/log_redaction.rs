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

// Redaction is process-wide and has to be set before connecting, so these cases live in their own
// test binary rather than beside tests that assume redaction is off, and run serially.

use couchbase_core::log_redaction::{is_log_redaction_enabled, set_log_redaction};
use couchbase_core::ondemand_agentmanager::OnDemandAgentManager;
use couchbase_core::options::ondemand_agentmanager::OnDemandAgentManagerOptions;
use serial_test::serial;

use crate::common::default_agent_options::create_default_options;
use crate::common::test_config::setup_test;

mod common;

#[serial]
#[test]
fn opening_an_agent_manager_applies_log_redaction() {
    setup_test(async |config| {
        set_log_redaction(false);

        let opts = OnDemandAgentManagerOptions::from(create_default_options(config).await)
            .log_redaction(true);
        let _mgr = OnDemandAgentManager::new(opts).await.unwrap();

        assert!(is_log_redaction_enabled());
        set_log_redaction(false);
    });
}

#[serial]
#[test]
fn opening_an_agent_manager_never_turns_log_redaction_off() {
    setup_test(async |config| {
        set_log_redaction(true);

        let opts = OnDemandAgentManagerOptions::from(create_default_options(config).await);
        let _mgr = OnDemandAgentManager::new(opts).await.unwrap();

        assert!(is_log_redaction_enabled());
        set_log_redaction(false);
    });
}
