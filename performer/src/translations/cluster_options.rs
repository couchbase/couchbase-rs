use crate::errors;
use crate::proto::protocol::shared::{
    authenticator, ClusterConfig, ClusterConnectionCreateRequest,
};
use couchbase::authenticator::{
    Authenticator, CertificateAuthenticator, JwtAuthenticator, PasswordAuthenticator,
};
use couchbase::options::cluster_options::{ClusterOptions, KvOptions, TlsOptions};
use std::io::Cursor;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub fn map_authenticator(auth: &authenticator::Authenticator) -> Authenticator {
    match auth {
        authenticator::Authenticator::PasswordAuth(auth) => Authenticator::PasswordAuthenticator(
            PasswordAuthenticator::new(auth.username.clone(), auth.password.clone()),
        ),
        authenticator::Authenticator::JwtAuth(auth) => {
            Authenticator::JwtAuthenticator(JwtAuthenticator::new(auth.jwt.clone()))
        }
        authenticator::Authenticator::CertificateAuth(auth) => {
            let root_cert = CertificateDer::from_pem_slice(auth.cert.as_bytes())
                .expect("Failed to parse certificate");
            let client_key = PrivateKeyDer::from_pem_slice(auth.key.as_bytes())
                .expect("Failed to parse private key");
            Authenticator::CertificateAuthenticator(CertificateAuthenticator::new(
                vec![root_cert],
                client_key,
            ))
        }
    }
}

pub fn build_authenticator(
    req: &ClusterConnectionCreateRequest,
) -> Result<Authenticator, errors::error::Error> {
    if let Some(auth) = &req.authenticator {
        if let Some(auth) = &auth.authenticator {
            let a = map_authenticator(auth);
            return Ok(a);
        }
    }

    Ok(Authenticator::PasswordAuthenticator(
        PasswordAuthenticator::new(req.cluster_username.clone(), req.cluster_password.clone()),
    ))
}

pub fn cluster_options(
    authenticator: Authenticator,
    opts: Option<&ClusterConfig>,
) -> crate::cluster::options::ClusterOptions {
    let mut cluster_options = ClusterOptions::new(authenticator);

    let timeout_options = if let Some(opts) = opts {
        let mut kv_options = KvOptions::new();
        if let Some(connect_timeout) = opts.kv_connect_timeout_secs {
            kv_options =
                kv_options.connect_timeout(std::time::Duration::from_secs(connect_timeout as u64));
        }

        cluster_options = cluster_options.kv_options(kv_options);

        if opts.insecure.is_some() || opts.cert.is_some() {
            let mut tls_options =
                TlsOptions::new().danger_accept_invalid_certs(opts.insecure.unwrap_or_default());
            if let Some(cert) = &opts.cert {
                let mut cursor = Cursor::new(cert.as_bytes());
                let first = rustls_pemfile::certs(&mut cursor)
                    .next()
                    .expect("No certificate found");
                tls_options =
                    tls_options.add_ca_certificate(first.expect("Certificate parsing failed"));
            }

            cluster_options = cluster_options.tls_options(tls_options);
        }

        crate::cluster::options::TimeoutOptions {
            kv_timeout: std::time::Duration::from_millis(
                opts.kv_timeout_millis.unwrap_or(2500) as u64
            ),
            kv_durable_timeout: std::time::Duration::from_millis(
                opts.kv_durable_timeout_millis.unwrap_or(10000) as u64,
            ),
            query_timeout: std::time::Duration::from_secs(
                opts.query_timeout_secs.unwrap_or(75) as u64
            ),
            search_timeout: std::time::Duration::from_secs(
                opts.search_timeout_secs.unwrap_or(75) as u64
            ),
            management_timeout: std::time::Duration::from_secs(
                opts.management_timeout_secs.unwrap_or(75) as u64,
            ),
        }
    } else {
        crate::cluster::options::TimeoutOptions {
            kv_timeout: std::time::Duration::from_millis(2500),
            kv_durable_timeout: std::time::Duration::from_millis(10000),
            query_timeout: std::time::Duration::from_millis(75000),
            search_timeout: std::time::Duration::from_millis(75000),
            management_timeout: std::time::Duration::from_millis(75000),
        }
    };

    crate::cluster::options::ClusterOptions {
        cluster_options,
        timeout_options,
    }
}
