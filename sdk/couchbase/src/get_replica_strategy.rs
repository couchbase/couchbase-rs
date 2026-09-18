//! Strategies describing how [`Collection::get_replica`](crate::collection::Collection::get_replica)
//! selects a replica to read from.

/// Describes how a replica read selects the replica (or replicas) it reads from.
///
/// A strategy is built through one of the associated functions, e.g.
/// [`GetReplicaStrategy::from_index`].
///
/// # Example
///
/// ```rust,no_run
/// # use couchbase::collection::Collection;
/// # use couchbase::get_replica_strategy::{GetReplicaStrategy, ReplicaIndex};
/// # async fn example(collection: Collection) -> couchbase::error::Result<()> {
/// let strategy = GetReplicaStrategy::from_index(ReplicaIndex::First, None);
/// let result = collection.get_replica("user::1", strategy, None).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct GetReplicaStrategy {
    kind: GetReplicaStrategyKind,
}

impl GetReplicaStrategy {
    /// Reads from the replica at `index`.
    ///
    /// By default, if the bucket has fewer replicas than `index` requires, the operation
    /// fails with [`ErrorKind::ReplicaIndexOutOfBounds`](crate::error::ErrorKind), and if
    /// the chosen replica currently has no node assigned (e.g. mid-rebalance/failover),
    /// it fails with [`ErrorKind::ReplicaIndexCurrentlyUnavailable`](crate::error::ErrorKind).
    /// Set [`GetReplicaStrategyFromIndexOptions::wrap`] to instead wrap around the
    /// available replicas in both cases: an out-of-bounds index is normalized modulo the
    /// replica count, and an unavailable replica is skipped in favor of the next one in
    /// the chain.
    pub fn from_index(
        index: ReplicaIndex,
        options: impl Into<Option<GetReplicaStrategyFromIndexOptions>>,
    ) -> Self {
        let options = options.into().unwrap_or_default();

        Self {
            kind: GetReplicaStrategyKind::FromIndex(FromIndexStrategy {
                index,
                wrap: options.wrap.unwrap_or(false),
            }),
        }
    }

    pub(crate) fn kind(&self) -> &GetReplicaStrategyKind {
        &self.kind
    }
}

#[derive(Debug, Clone)]
pub(crate) enum GetReplicaStrategyKind {
    FromIndex(FromIndexStrategy),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FromIndexStrategy {
    pub(crate) index: ReplicaIndex,
    pub(crate) wrap: bool,
}

/// Options for [`GetReplicaStrategy::from_index`].
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetReplicaStrategyFromIndexOptions {
    /// If `true`, wrap around the replicas available on the bucket rather than failing
    /// when the requested index is out of bounds. Defaults to `false`.
    pub wrap: Option<bool>,
}

impl GetReplicaStrategyFromIndexOptions {
    /// Creates a new `GetReplicaStrategyFromIndexOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// If `true`, wrap around the replicas available on the bucket rather than failing
    /// when the requested index is out of bounds.
    pub fn wrap(mut self, wrap: bool) -> Self {
        self.wrap = Some(wrap);
        self
    }
}

/// Identifies a replica of a vbucket, in the order the cluster map lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReplicaIndex {
    First,
    Second,
    Third,
}

impl ReplicaIndex {
    /// The index of this replica within a vbucket's server list, where `0` is the active.
    /// [`ReplicaIndex::First`] is therefore `1`.
    pub(crate) fn vbucket_server_index(self) -> u32 {
        match self {
            ReplicaIndex::First => 1,
            ReplicaIndex::Second => 2,
            ReplicaIndex::Third => 3,
        }
    }
}
