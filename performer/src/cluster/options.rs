use std::time::Duration;

pub struct ClusterOptions {
    pub cluster_options: couchbase::options::cluster_options::ClusterOptions,
    #[allow(dead_code)]
    pub timeout_options: TimeoutOptions,
}

#[allow(dead_code)]
pub struct TimeoutOptions {
    pub kv_timeout: Duration,
    pub kv_durable_timeout: Duration,
    pub query_timeout: Duration,
    pub search_timeout: Duration,
    pub management_timeout: Duration,
}
