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

//! Helpers backing [`crate::crudcomponent::CrudComponent::get_replica`]'s replica
//! selection (see [`crate::options::crud::GetReplicaStrategy`]).

use crate::error::{Error, ErrorKind};

/// Computes the next replica index to retry with for the `wrap` replica-read strategy,
/// given a dispatch failure for `requested_index` (`0` = active, `1..=3` = replica).
/// Returns `None` if there's nowhere left to go, in which case the original error
/// should be surfaced as-is.
pub(crate) fn next_replica_index_to_try(
    err: &Error,
    requested_index: u32,
    hops_left: &mut Option<u32>,
) -> Option<u32> {
    match err.kind() {
        ErrorKind::InvalidReplica { num_servers, .. } => {
            let num_replicas = num_servers.saturating_sub(1) as u32;
            if num_replicas == 0 {
                return None;
            }
            Some(((requested_index - 1) % num_replicas) + 1)
        }
        ErrorKind::NoServerAssigned {
            vb_server_idx,
            num_replicas,
            ..
        } if *vb_server_idx > 0 => {
            let num_replicas = *num_replicas as u32;
            if num_replicas == 0 {
                return None;
            }
            let next_index = (requested_index % num_replicas) + 1;

            if next_index == requested_index {
                return None;
            }

            let remaining = hops_left.get_or_insert(num_replicas.saturating_sub(1));
            if *remaining == 0 {
                return None;
            }
            *remaining -= 1;

            Some(next_index)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::next_replica_index_to_try;
    use crate::error::{Error, ErrorKind};

    fn out_of_bounds(num_servers: usize) -> Error {
        ErrorKind::InvalidReplica {
            requested_replica: 0,
            num_servers,
        }
        .into()
    }

    fn unavailable(num_replicas: usize) -> Error {
        ErrorKind::NoServerAssigned {
            requested_vb_id: 0,
            vb_server_idx: 1,
            num_replicas,
        }
        .into()
    }

    #[test]
    fn out_of_bounds_wraps_via_modulo() {
        let err = out_of_bounds(3);
        let mut hops_left = None;
        assert_eq!(Some(1), next_replica_index_to_try(&err, 3, &mut hops_left));
    }

    #[test]
    fn out_of_bounds_with_zero_replicas_does_not_wrap() {
        let err = out_of_bounds(1);
        let mut hops_left = None;
        assert_eq!(None, next_replica_index_to_try(&err, 1, &mut hops_left));
    }

    #[test]
    fn out_of_bounds_wraps_even_with_a_single_replica() {
        let err = out_of_bounds(2);
        let mut hops_left = None;
        assert_eq!(Some(1), next_replica_index_to_try(&err, 2, &mut hops_left));
    }

    #[test]
    fn unavailable_steps_to_next_replica() {
        let err = unavailable(3);
        let mut hops_left = None;
        assert_eq!(Some(2), next_replica_index_to_try(&err, 1, &mut hops_left));
    }

    #[test]
    fn unavailable_wraps_past_last_replica_to_first() {
        let err = unavailable(3);
        let mut hops_left = None;
        assert_eq!(Some(1), next_replica_index_to_try(&err, 3, &mut hops_left));
    }

    #[test]
    fn unavailable_single_replica_has_nowhere_to_go() {
        let err = unavailable(1);
        let mut hops_left = None;
        assert_eq!(None, next_replica_index_to_try(&err, 1, &mut hops_left));
    }

    #[test]
    fn stops_after_one_full_lap() {
        let mut hops_left = None;
        let mut index = 1u32;
        let mut visited = vec![index];

        loop {
            let err = unavailable(3);
            match next_replica_index_to_try(&err, index, &mut hops_left) {
                Some(next) => {
                    index = next;
                    visited.push(index);
                }
                None => break,
            }
        }

        assert_eq!(vec![1, 2, 3], visited);
    }

    #[test]
    fn non_replica_errors_are_left_alone() {
        let err: Error = ErrorKind::NoBucket.into();
        let mut hops_left = None;
        assert_eq!(None, next_replica_index_to_try(&err, 1, &mut hops_left));
    }
}
