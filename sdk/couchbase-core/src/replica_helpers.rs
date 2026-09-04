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
//!
//! Named generically, rather than e.g. `replica_wrap`, so that future strategies (the
//! GetReplica RFC mentions a possible future `fastest()`, for instance) have an obvious
//! home here alongside `wrap`'s helpers, instead of needing their own similarly-named
//! sibling module.

use crate::error::{Error, ErrorKind};

/// Given a failure encountered while dispatching to a specific node in a vbucket's
/// server list (`requested_index`: `0` would be the active node, `1..=3` a replica —
/// only replica indexes are meaningful here), computes the next replica index a caller
/// implementing a wrapping replica-read strategy should retry with, if any.
///
/// This captures the two vbucket-map-topology failures the GetReplica RFC's `wrap`
/// option needs to handle, quite differently:
///
/// - [`ErrorKind::InvalidReplica`]: the requested index is structurally beyond the
///   bucket's replica count, so it never referred to a real replica in the first
///   place. Normalizing it modulo the replica count (e.g. requesting the third replica
///   of a bucket with only one replica normalizes to the first) is a one-time, always
///   -safe correction, not "trying a different replica" — it doesn't consume the lap
///   budget below, which is what a single-replica bucket needs to still wrap correctly
///   (there'd be no attempts left to spend otherwise).
/// - [`ErrorKind::NoServerAssigned`] (for a replica index, i.e. `vb_server_idx > 0`):
///   the chosen replica -- a real one -- currently has no node assigned (a `-1` entry
///   in the vbucket map, e.g. mid-rebalance/failover). Step forward to the next
///   replica in the chain, wrapping past the last back to the first, bounded by
///   `hops_left` to at most one full lap so a chain that is entirely unavailable
///   terminates rather than looping forever.
///
/// Returns `None` when there is nowhere useful to move to, in which case the original
/// error should be surfaced as-is.
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

            // Landing back on the index we just tried means there's nowhere left to
            // go (e.g. a single-replica chain where that one replica is down).
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
        // Bucket has 2 replicas (num_servers = active + replicas); THIRD (3) wraps to
        // FIRST (1), matching the RFC's worked example.
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
        // 1 replica (num_servers = 2): requesting SECOND (2) is out of bounds, and
        // must still wrap to FIRST (1) per the RFC's modulo rule -- this must not be
        // blocked by the same lap budget that bounds `NoServerAssigned` hops, since
        // normalizing an out-of-bounds index isn't "trying a different replica".
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
        // Only one replica exists, and it's the one that's down.
        let err = unavailable(1);
        let mut hops_left = None;
        assert_eq!(None, next_replica_index_to_try(&err, 1, &mut hops_left));
    }

    #[test]
    fn stops_after_one_full_lap() {
        // A 3-replica chain that's entirely unavailable should be given up on after
        // trying every replica once, not retried forever.
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

        // One full lap: the starting replica plus the other two, no repeats, then stop.
        assert_eq!(vec![1, 2, 3], visited);
    }

    #[test]
    fn non_replica_errors_are_left_alone() {
        let err: Error = ErrorKind::NoBucket.into();
        let mut hops_left = None;
        assert_eq!(None, next_replica_index_to_try(&err, 1, &mut hops_left));
    }
}
