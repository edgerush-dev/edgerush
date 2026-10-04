//! The tests of serving, by what they are about. What more than one of them uses is in
//! `workers`, `clients`, `upstreams` and `configs`.

use super::*;
use crate::connections::{Loads, QUIC_MOST};
use crate::downstream::h1::deadlines::Bounds;
use crate::downstream::h3::listener::Forwarding;
use crate::forwarding::Client;
use crate::linger::Lent;
use crate::metrics::{Answer, Metrics, Stopped};
use crate::proxy_protocol;
use crate::tls::Tls;
use crate::upstream::destination::{Keys, ReuseIdentity};
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::codec::Sending;
use crate::upstream::h1::exchange::{ExchangeError, H1Body};
use crate::upstream::secure::Socket as UpstreamSocket;
use crate::websocket::Key;
use bytes::Bytes;
use edgerush_config::{Compiled, CompiledRetry, Config, UpstreamProtocol, compile};
use http::uri::Authority;
use http::{HeaderValue, Method, Request, Response, StatusCode, Uri, Version};
use http_body_util::{BodyExt, Empty};
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::Instant;

mod clients;
mod configs;
mod upstreams;
mod workers;

mod deadlines;
mod drain;
mod grpc;
mod headers;
mod health;
mod http2;
mod http3;
mod mirrors;
mod passthrough;
mod plane;
mod proxy_headers;
mod resources;
mod retries;
mod timeouts;
mod tls;
mod upstream_h2;
mod upstream_tls;
mod websocket;

use clients::*;
use configs::*;
use upstreams::*;
use workers::*;
