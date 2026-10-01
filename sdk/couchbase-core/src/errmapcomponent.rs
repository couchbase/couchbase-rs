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

use crate::errmap::{parse_error_map, ErrMap};
use crate::log_redaction::not_sensitive;
use crate::memdx::status::Status;
use arc_swap::{ArcSwapOption, AsRaw};
use std::ptr;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug)]
pub struct ErrMapComponent {
    err_map: ArcSwapOption<ErrMap>,
}

impl Default for ErrMapComponent {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrMapComponent {
    pub fn new() -> Self {
        Self {
            err_map: ArcSwapOption::from(None),
        }
    }

    pub(crate) fn on_err_map(&self, err_map: &[u8]) {
        match parse_error_map(err_map) {
            Ok(err_map) => {
                let new_err_map = Arc::new(err_map);

                loop {
                    let mut current_err_map = self.err_map.load();

                    match current_err_map.as_ref() {
                        Some(cem) => {
                            if new_err_map.revision <= cem.revision {
                                break;
                            }

                            debug!(
                                "Attempting to apply new error map: {}",
                                new_err_map.revision
                            );

                            let prev = self
                                .err_map
                                .compare_and_swap(&current_err_map, Some(new_err_map.clone()));

                            if !ptr::eq(prev.as_raw(), current_err_map.as_raw()) {
                                break;
                            }
                        }
                        None => {
                            self.err_map.store(Some(new_err_map));
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                // A parse error describes the error map the server sent, which carries no
                // application data.
                tracing::info!("Failed to parse error map: {}", not_sensitive(&e));
            }
        }
    }

    pub fn should_retry(&self, status: &Status) -> bool {
        let err_map = self.err_map.load();
        if let Some(err_map) = err_map.as_ref() {
            if let Some(err_data) = err_map.errors.get(&status.into()) {
                for attr in &err_data.attributes {
                    if attr == "retry-now" || attr == "retry-later" || attr == "auto-retry" {
                        return true;
                    }
                }
            }
        }

        false
    }
}
