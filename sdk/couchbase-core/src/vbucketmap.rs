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

use crate::error::Result;
use crate::error::{Error, ErrorKind};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct VbucketMap {
    entries: Vec<Vec<i16>>,
    num_replicas: usize,
}

impl VbucketMap {
    pub fn new(entries: Vec<Vec<i16>>, num_replicas: usize) -> Result<Self> {
        if entries.is_empty() {
            return Err(Error::new_message_error(
                "vbucket map must have at least a single entry",
            ));
        }

        Ok(Self {
            entries,
            num_replicas,
        })
    }

    pub fn is_valid(&self) -> bool {
        if let Some(entry) = self.entries.first() {
            return !entry.is_empty();
        }

        false
    }

    pub fn num_vbuckets(&self) -> usize {
        self.entries.len()
    }

    pub fn num_replicas(&self) -> usize {
        self.num_replicas
    }

    pub fn vbucket_by_key(&self, key: &[u8]) -> u16 {
        let checksum = crc32fast::hash(key);
        let mid_bits = (checksum >> 16) as u16 & 0x7fff;
        mid_bits % (self.entries.len() as u16)
    }

    pub fn node_by_vbucket(&self, vb_id: u16, vb_server_idx: u32) -> Result<i16> {
        let num_servers = (self.num_replicas as u32) + 1;
        if vb_server_idx >= num_servers {
            return Err(ErrorKind::InvalidReplica {
                requested_replica: vb_server_idx,
                num_servers: num_servers as usize,
            }
            .into());
        }

        if let Some(idx) = self.entries.get(vb_id as usize) {
            if let Some(id) = idx.get(vb_server_idx as usize) {
                Ok(*id)
            } else {
                Ok(-1)
            }
        } else {
            Err(ErrorKind::InvalidVbucket {
                requested_vb_id: vb_id,
                num_vbuckets: self.entries.len(),
            }
            .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ErrorKind;
    use crate::vbucketmap::VbucketMap;

    #[test]
    fn node_by_vbucket_valid_positions() {
        let vb_map = VbucketMap::new(vec![vec![7, 3, -1]], 2).unwrap();

        assert_eq!(7, vb_map.node_by_vbucket(0, 0).unwrap());
        assert_eq!(3, vb_map.node_by_vbucket(0, 1).unwrap());
        assert_eq!(-1, vb_map.node_by_vbucket(0, 2).unwrap());
    }

    #[test]
    fn node_by_vbucket_one_past_the_last_replica_is_out_of_bounds() {
        let vb_map = VbucketMap::new(vec![vec![7, 3, -1]], 2).unwrap();

        let err = vb_map.node_by_vbucket(0, 3).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidReplica {
                requested_replica: 3,
                num_servers: 3,
            }
        ));
    }

    #[test]
    fn node_by_vbucket_well_past_the_last_replica_is_out_of_bounds() {
        let vb_map = VbucketMap::new(vec![vec![7, 3, -1]], 2).unwrap();

        let err = vb_map.node_by_vbucket(0, 100).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidReplica { .. }));
    }

    #[test]
    fn vbucketmap_with_1024_vbs() {
        let vb_map = VbucketMap::new(vec![vec![]; 1024], 1).unwrap();

        assert_eq!(0x0202u16, vb_map.vbucket_by_key(vec![0].as_slice()));
        assert_eq!(
            0x00aau16,
            vb_map.vbucket_by_key(vec![0, 1, 2, 3, 4, 5, 6, 7].as_slice())
        );
        assert_eq!(0x0210u16, vb_map.vbucket_by_key(b"hello"));
        assert_eq!(
            0x03d4u16,
            vb_map.vbucket_by_key(b"hello world, I am a super long key lets see if it works")
        );
    }

    #[test]
    fn vbucketmap_with_64_vbs() {
        let vb_map = VbucketMap::new(vec![vec![]; 64], 1).unwrap();

        assert_eq!(0x0002u16, vb_map.vbucket_by_key(vec![0].as_slice()));
        assert_eq!(
            0x002au16,
            vb_map.vbucket_by_key(vec![0, 1, 2, 3, 4, 5, 6, 7].as_slice())
        );
        assert_eq!(0x0010u16, vb_map.vbucket_by_key(b"hello"));
        assert_eq!(
            0x0014u16,
            vb_map.vbucket_by_key(b"hello world, I am a super long key lets see if it works")
        );
    }

    #[test]
    fn vbucketmap_with_48_vbs() {
        let vb_map = VbucketMap::new(vec![vec![]; 48], 1).unwrap();

        assert_eq!(0x0012u16, vb_map.vbucket_by_key(vec![0].as_slice()));
        assert_eq!(
            0x000au16,
            vb_map.vbucket_by_key(vec![0, 1, 2, 3, 4, 5, 6, 7].as_slice())
        );
        assert_eq!(0x0010u16, vb_map.vbucket_by_key(b"hello"));
        assert_eq!(
            0x0004u16,
            vb_map.vbucket_by_key(b"hello world, I am a super long key lets see if it works")
        );
    }

    #[test]
    fn vbucketmap_with_13_vbs() {
        let vb_map = VbucketMap::new(vec![vec![]; 13], 1).unwrap();

        assert_eq!(0x000cu16, vb_map.vbucket_by_key(vec![0].as_slice()));
        assert_eq!(
            0x0008u16,
            vb_map.vbucket_by_key(vec![0, 1, 2, 3, 4, 5, 6, 7].as_slice())
        );
        assert_eq!(0x0008u16, vb_map.vbucket_by_key(b"hello"));
        assert_eq!(
            0x0003u16,
            vb_map.vbucket_by_key(b"hello world, I am a super long key lets see if it works")
        );
    }
}
