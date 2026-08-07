// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::io::{IoSlice, IoSliceMut};
use std::net::{Shutdown, SocketAddr};

use socket2::SockRef;
use tokio::net::TcpStream;

use crate::Transport;

/// Raw TCP byte transport.
pub struct TcpTransport {
    stream: TcpStream,
}

impl TcpTransport {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn connect_endpoint(remote: SocketAddr) -> io::Result<Self> {
        let socket = if remote.is_ipv4() {
            tokio::net::TcpSocket::new_v4()?
        } else {
            tokio::net::TcpSocket::new_v6()?
        };
        socket.set_nodelay(true)?;
        socket.connect(remote).await.map(Self::new)
    }

    pub async fn readable(&self) -> io::Result<()> {
        self.stream.readable().await
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.stream.writable().await
    }

    pub fn shutdown(&self) -> io::Result<()> {
        match SockRef::from(&self.stream).shutdown(Shutdown::Both) {
            Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
            result => result,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_write(&self, source: &[u8]) -> io::Result<usize> {
        self.stream.try_write(source)
    }
}

impl Transport for TcpTransport {
    #[inline]
    fn try_read_vectored(&self, destination: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.stream.try_read_vectored(destination)
    }

    #[inline]
    fn try_write_vectored(&self, source: &[IoSlice<'_>]) -> io::Result<usize> {
        self.stream.try_write_vectored(source)
    }
}
