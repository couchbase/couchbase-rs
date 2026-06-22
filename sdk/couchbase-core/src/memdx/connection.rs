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

use crate::memdx::error::Error;
use crate::memdx::error::Result;
use crate::tls_config::TlsConfig;
use socket2::TcpKeepalive;
use std::fmt::Debug;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::{timeout_at, Instant};

use crate::address::Address;
#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use {
    tokio_rustls::rustls::pki_types::DnsName, tokio_rustls::rustls::pki_types::ServerName,
    tokio_rustls::TlsConnector,
};

#[derive(Debug)]
pub struct ConnectOptions {
    pub deadline: Instant,
    pub tcp_keep_alive_time: Duration,
}

pub trait Stream: Debug + AsyncWrite + AsyncRead + Send + Sync + Unpin + 'static {}

impl Stream for TcpStream {}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ConnectionType {
    Tcp(TcpConnection),
    Tls(TlsConnection),
}

impl ConnectionType {
    pub fn into_inner(self) -> Box<dyn Stream> {
        match self {
            ConnectionType::Tcp(connection) => Box::new(connection.stream),
            ConnectionType::Tls(connection) => Box::new(connection.stream),
        }
    }

    pub fn local_addr(&self) -> &SocketAddr {
        match self {
            ConnectionType::Tcp(connection) => &connection.local_addr,
            ConnectionType::Tls(connection) => &connection.local_addr,
        }
    }

    pub fn peer_addr(&self) -> &SocketAddr {
        match self {
            ConnectionType::Tcp(connection) => &connection.peer_addr,
            ConnectionType::Tls(connection) => &connection.peer_addr,
        }
    }
}

#[derive(Debug)]
pub struct TcpConnection {
    stream: TcpStream,

    local_addr: SocketAddr,
    peer_addr: SocketAddr,
}

impl TcpConnection {
    async fn tcp_stream(
        addr: &str,
        opts: &ConnectOptions,
    ) -> Result<(TcpStream, SocketAddr, SocketAddr)> {
        let tcp_socket = timeout_at(opts.deadline, TcpStream::connect(addr))
            .await
            .map_err(|e| {
                Error::new_connection_failed_error(
                    "failed to connect to server within timeout",
                    Box::new(io::Error::new(io::ErrorKind::TimedOut, e)),
                )
            })?
            .map_err(|e| {
                Error::new_connection_failed_error("failed to create tcp stream", Box::new(e))
            })?;

        let local_addr = tcp_socket
            .local_addr()
            .unwrap_or_else(|_e| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0));

        let peer_addr = tcp_socket
            .peer_addr()
            .unwrap_or_else(|_e| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0));

        // Tokio doesn't expose a keep alive function, but they just call into socket2 for set_linger.
        socket2::SockRef::from(&tcp_socket)
            .set_tcp_keepalive(&TcpKeepalive::new().with_time(opts.tcp_keep_alive_time))?;

        tcp_socket.set_nodelay(false).map_err(|e| {
            Error::new_connection_failed_error("failed to set tcp nodelay", Box::new(e))
        })?;

        Ok((tcp_socket, local_addr, peer_addr))
    }

    pub async fn connect(addr: Address, opts: ConnectOptions) -> Result<TcpConnection> {
        let (stream, local_addr, peer_addr) =
            TcpConnection::tcp_stream(addr.to_string().as_str(), &opts).await?;

        Ok(TcpConnection {
            stream,
            local_addr,
            peer_addr,
        })
    }

    fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    fn peer_addr(&self) -> &SocketAddr {
        &self.peer_addr
    }
}

#[derive(Debug)]
pub struct TlsConnection {
    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    stream: tokio_rustls::client::TlsStream<TcpStream>,
    #[cfg(feature = "native-tls")]
    stream: tokio_native_tls::TlsStream<TcpStream>,

    local_addr: SocketAddr,
    peer_addr: SocketAddr,
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl Stream for tokio_rustls::client::TlsStream<TcpStream> {}

#[cfg(feature = "native-tls")]
impl Stream for tokio_native_tls::TlsStream<TcpStream> {}

impl TlsConnection {
    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub async fn connect(
        addr: Address,
        tls_config: TlsConfig,
        opts: ConnectOptions,
    ) -> Result<TlsConnection> {
        let (tcp_socket, local_addr, peer_addr) =
            TcpConnection::tcp_stream(addr.to_string().as_str(), &opts).await?;

        let connector = TlsConnector::from(tls_config);

        let server_name = match DnsName::try_from(addr.host) {
            Ok(name) => ServerName::DnsName(name),
            Err(_e) => ServerName::IpAddress(tokio_rustls::rustls::pki_types::IpAddr::from(
                peer_addr.ip(),
            )),
        };

        let stream = timeout_at(opts.deadline, connector.connect(server_name, tcp_socket))
            .await
            .map_err(|e| {
                Error::new_connection_failed_error(
                    "failed to upgrade tcp stream to tls within timeout",
                    Box::new(io::Error::new(io::ErrorKind::TimedOut, e)),
                )
            })?
            .map_err(|e| {
                Error::new_connection_failed_error(
                    "failed to upgrade tcp stream to tls",
                    Box::new(e),
                )
            })?;

        Ok(TlsConnection {
            stream,
            local_addr,
            peer_addr,
        })
    }

    #[cfg(feature = "native-tls")]
    pub async fn connect(
        addr: Address,
        tls_config: TlsConfig,
        opts: ConnectOptions,
    ) -> Result<TlsConnection> {
        let (tcp_socket, local_addr, peer_addr) =
            TcpConnection::tcp_stream(addr.to_string().as_str(), &opts).await?;

        let tls_connector = tokio_native_tls::TlsConnector::from(tls_config);

        let remote_addr = addr.to_string();
        let stream = timeout_at(
            opts.deadline,
            tls_connector.connect(&remote_addr, tcp_socket),
        )
        .await
        .map_err(|e| {
            Error::new_connection_failed_error(
                "failed to upgrade tcp stream to tls within timeout",
                Box::new(io::Error::new(io::ErrorKind::TimedOut, e)),
            )
        })?
        .map_err(|e| {
            Error::new_connection_failed_error(
                "failed to upgrade tcp stream to tls",
                Box::new(io::Error::other(e)),
            )
        })?;

        Ok(TlsConnection {
            stream,
            local_addr,
            peer_addr,
        })
    }

    fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    fn peer_addr(&self) -> &SocketAddr {
        &self.peer_addr
    }
}
