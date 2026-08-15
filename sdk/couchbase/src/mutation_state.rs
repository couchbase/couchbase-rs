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

//! Mutation state tracking for scan consistency.
//!
//! A [`MutationState`] aggregates [`MutationToken`]s from mutation results so that
//! subsequent queries or scans can wait for those mutations to be indexed.
//! Pass it via [`QueryOptions::scan_consistency`](crate::options::query_options::QueryOptions::scan_consistency)
//! with [`ScanConsistency::AtPlus`](crate::options::query_options::ScanConsistency::AtPlus).

use couchbase_core::queryx::query_options::{ScanVectorEntry, SparseScanVectors};
use serde::de::{MapAccess, Visitor};
use serde::ser::{SerializeMap, SerializeStruct};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Write;
use std::sync::Arc;

/// A token representing a specific mutation on a specific vBucket.
///
/// Obtained from [`MutationResult::mutation_token`](crate::results::kv_results::MutationResult::mutation_token)
/// after a successful mutation. Used to build a [`MutationState`] for scan consistency
/// in operations like query.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MutationToken {
    pub(crate) token: couchbase_core::mutationtoken::MutationToken,
    pub(crate) bucket_name: Arc<str>,
}

impl MutationToken {
    pub(crate) fn new(
        token: couchbase_core::mutationtoken::MutationToken,
        bucket_name: Arc<str>,
    ) -> Self {
        Self { token, bucket_name }
    }

    #[cfg(feature = "internal")]
    pub fn from_parts(
        vbid: u16,
        vbuuid: u64,
        seqno: u64,
        bucket_name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            token: couchbase_core::mutationtoken::MutationToken::new(vbid, vbuuid, seqno),
            bucket_name: bucket_name.into(),
        }
    }

    /// Returns the vBucket partition ID.
    pub fn partition_id(&self) -> u16 {
        self.token.vbid()
    }

    /// Returns the vBucket UUID.
    pub fn partition_uuid(&self) -> u64 {
        self.token.vbuuid()
    }

    /// Returns the sequence number of this mutation.
    pub fn sequence_number(&self) -> u64 {
        self.token.seqno()
    }

    /// Returns the name of the bucket this token belongs to.
    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }
}

/// Aggregates [`MutationToken`]s to achieve read-your-own-writes scan consistency
/// in operations like SQL++ query.
///
/// Build a `MutationState` by pushing tokens from mutation results, then pass it to
/// [`ScanConsistency::AtPlus`](crate::options::query_options::ScanConsistency::AtPlus).
///
/// # Example
///
/// ```rust,no_run
/// # use couchbase::collection::Collection;
/// # use couchbase::mutation_state::MutationState;
/// # use couchbase::options::query_options::{QueryOptions, ScanConsistency};
/// # async fn example(collection: Collection, cluster: couchbase::cluster::Cluster) -> couchbase::error::Result<()> {
/// let result = collection.upsert("key", &serde_json::json!({"x": 1}), None).await?;
///
/// let mut state = MutationState::new();
/// if let Some(token) = result.mutation_token() {
///     state = state.push_token(token.clone());
/// }
///
/// let opts = QueryOptions::new()
///     .scan_consistency(ScanConsistency::AtPlus(state));
/// let mut rows = cluster.query("SELECT * FROM `bucket` WHERE x = 1", opts).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct MutationState {
    // There's a bit of duplication between key and value here but that allows us to recreate
    // the tokens easily.
    tokens: HashMap<MutationStateKey, couchbase_core::mutationtoken::MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct MutationStateKey {
    bucket_name: Arc<str>,
    vbid: u16,
}

impl Serialize for MutationStateKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&*self.bucket_name, &self.vbid)?;
        map.end()
    }
}

impl MutationState {
    /// Creates a new empty `MutationState`.
    pub fn new() -> Self {
        Self {
            tokens: HashMap::default(),
        }
    }

    /// Adds a mutation token to this state. If a token for the same vBucket already
    /// exists, only the one with the higher sequence number is kept.
    ///
    /// Returns `self` for chaining.
    pub fn push_token(mut self, token: MutationToken) -> Self {
        let key = MutationStateKey {
            bucket_name: token.bucket_name,
            vbid: token.token.vbid(),
        };

        if let Some(entry) = self.tokens.get(&key) {
            if entry.seqno() < token.token.seqno() {
                self.tokens.insert(key, token.token);
            }
        } else {
            self.tokens.insert(key, token.token);
        }

        self
    }

    /// Returns all tokens in this mutation state.
    pub fn tokens(&self) -> Vec<MutationToken> {
        self.tokens
            .iter()
            .map(|(key, token)| MutationToken::new(token.clone(), key.bucket_name.clone()))
            .collect()
    }
}

impl From<MutationToken> for MutationState {
    fn from(value: MutationToken) -> Self {
        MutationState::new().push_token(value)
    }
}

impl From<Vec<MutationToken>> for MutationState {
    fn from(value: Vec<MutationToken>) -> Self {
        let mut state = MutationState::new();
        for token in value {
            state = state.push_token(token);
        }
        state
    }
}

impl From<MutationState> for HashMap<String, SparseScanVectors> {
    fn from(value: MutationState) -> Self {
        let mut buckets: HashMap<String, SparseScanVectors> = HashMap::default();
        for (key, token) in value.tokens {
            let bucket = buckets.entry(key.bucket_name.to_string()).or_default();
            bucket.insert(
                key.vbid.to_string(),
                ScanVectorEntry::new(token.seqno(), token.vbuuid().to_string()),
            );
        }

        buckets
    }
}

impl Serialize for MutationState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(None)?;
        let mut buckets: HashMap<String, HashMap<String, (u64, String)>> = HashMap::new();

        for (key, token) in &self.tokens {
            let bucket = buckets.entry(key.bucket_name.to_string()).or_default();
            bucket.insert(
                key.vbid.to_string(),
                (token.seqno(), token.vbuuid().to_string()),
            );
        }

        for (bucket_name, vbuckets) in buckets {
            state.serialize_entry(&bucket_name, &vbuckets)?;
        }

        state.end()
    }
}

impl<'de> Deserialize<'de> for MutationState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MutationStateVisitor;

        impl<'de> Visitor<'de> for MutationStateVisitor {
            type Value = MutationState;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map of bucket names to vbucket entries")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut tokens = HashMap::new();

                while let Some((bucket_name, vbuckets)) =
                    map.next_entry::<String, HashMap<String, (u64, String)>>()?
                {
                    let bucket_name = Arc::<str>::from(bucket_name);
                    for (vbid, (seqno, vbuuid)) in vbuckets {
                        let key = MutationStateKey {
                            bucket_name: bucket_name.clone(),
                            vbid: vbid.parse().map_err(de::Error::custom)?,
                        };
                        let token = couchbase_core::mutationtoken::MutationToken::new(
                            key.vbid,
                            vbuuid.parse().map_err(de::Error::custom)?,
                            seqno,
                        );
                        tokens.insert(key, token);
                    }
                }

                Ok(MutationState { tokens })
            }
        }

        deserializer.deserialize_map(MutationStateVisitor)
    }
}

#[macro_export]
macro_rules! mutation_state {
    ( $($x:expr),+ ) => {
        {
            let mut state = MutationState::new();
            $(
                state = state.push_token($x);
            )*
            state
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::mutation_state::MutationState;
    use crate::mutation_state::MutationToken;
    use std::sync::Arc;

    #[test]
    fn serialization() {
        let mutation_state = mutation_state! {
             MutationToken::new(
                couchbase_core::mutationtoken::MutationToken::new(1, 1234, 1),
                Arc::from("default"),
                ),
             MutationToken::new(
                couchbase_core::mutationtoken::MutationToken::new(25, 5678, 10),
                Arc::from("beer-sample"),
                )
        };

        let serialized = serde_json::to_string(&mutation_state).unwrap();
        assert!(serialized.contains(r#""default":{"1":[1,"1234"]}"#));
        assert!(serialized.contains(r#""beer-sample":{"25":[10,"5678"]}"#));
    }

    #[test]
    fn deserialization() {
        let json = r#"{"default":{"1":[1,"1234"]},"beer-sample":{"25":[10,"5678"]}}"#;

        let mutation_state: MutationState = serde_json::from_str(json).unwrap();
        let tokens = mutation_state.tokens();
        assert!(tokens.contains(&MutationToken::new(
            couchbase_core::mutationtoken::MutationToken::new(1, 1234, 1),
            Arc::from("default"),
        )));
        assert!(tokens.contains(&MutationToken::new(
            couchbase_core::mutationtoken::MutationToken::new(25, 5678, 10),
            Arc::from("beer-sample"),
        )));
    }
}
