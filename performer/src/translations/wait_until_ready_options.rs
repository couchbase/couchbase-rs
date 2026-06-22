use crate::errors::error::Error;
use crate::proto::protocol::sdk::cluster::wait_until_ready::{
    ClusterState, ServiceType, WaitUntilReadyOptions,
};

impl TryFrom<WaitUntilReadyOptions>
    for couchbase::options::diagnostic_options::WaitUntilReadyOptions
{
    type Error = Box<Error>;

    fn try_from(opts: WaitUntilReadyOptions) -> Result<Self, Self::Error> {
        let mut sdk_opts = couchbase::options::diagnostic_options::WaitUntilReadyOptions::new();

        let mut sdk_services = Vec::with_capacity(opts.service_types.len());
        for service_type in opts.service_types {
            match ServiceType::try_from(service_type).unwrap() {
                ServiceType::Kv => {
                    sdk_services.push(couchbase::service_type::ServiceType::KV);
                }
                ServiceType::Query => {
                    sdk_services.push(couchbase::service_type::ServiceType::QUERY);
                }
                ServiceType::Search => {
                    sdk_services.push(couchbase::service_type::ServiceType::SEARCH);
                }
                _ => {
                    return Err(Error::unimplemented("Unsupported service type"));
                }
            }
        }

        if !sdk_services.is_empty() {
            sdk_opts = sdk_opts.service_types(sdk_services);
        }

        if let Some(state) = opts.desired_state {
            match ClusterState::try_from(state).unwrap() {
                ClusterState::Online => {
                    sdk_opts = sdk_opts.desired_state(
                        couchbase::options::diagnostic_options::ClusterState::Online,
                    );
                }
                ClusterState::Degraded => {
                    sdk_opts = sdk_opts.desired_state(
                        couchbase::options::diagnostic_options::ClusterState::Degraded,
                    );
                }
                ClusterState::Offline => {
                    sdk_opts = sdk_opts.desired_state(
                        couchbase::options::diagnostic_options::ClusterState::Offline,
                    );
                }
            }
        }

        Ok(sdk_opts)
    }
}
