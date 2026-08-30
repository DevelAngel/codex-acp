//! Local TCP bridge that lets the `--acp-mcp-bridge` stdio MCP server
//! (spawned per session for a single `McpServer::Acp` declaration) relay
//! JSON-RPC MCP requests through the agent's ACP connection to the client,
//! which owns the actual MCP server.

use std::{net::SocketAddr, sync::Arc};

use agent_client_protocol::schema::v1::{ConnectMcpRequest, McpConnectionId, McpServerAcpId, MessageMcpRequest};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    sync::{mpsc::UnboundedSender, oneshot},
    task,
};
use tracing::{error, warn};

use crate::agent::ClientOp;

#[derive(Clone)]
pub struct AcpMcpBridge {
    address: SocketAddr,
    _inner: Arc<AcpMcpBridgeInner>,
}

impl AcpMcpBridge {
    pub async fn start(client_tx: UnboundedSender<ClientOp>) -> anyhow::Result<Arc<AcpMcpBridge>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let inner = Arc::new(AcpMcpBridgeInner { client_tx });

        let accept_inner = inner.clone();
        task::spawn_local(async move {
            let listener = listener;
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let connection_inner = accept_inner.clone();
                        task::spawn_local(async move {
                            if let Err(err) = handle_connection(stream, connection_inner).await {
                                warn!(error = %err, remote = %addr, "acp mcp bridge connection errored");
                            }
                        });
                    }
                    Err(err) => {
                        error!(error = %err, "acp mcp bridge listener failed");
                        break;
                    }
                }
            }
        });

        Ok(Arc::new(AcpMcpBridge {
            address,
            _inner: inner,
        }))
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

/// One forwarded MCP JSON-RPC call: the `--acp-mcp-bridge` process sends the
/// bare `method`/`params` pair it received on stdio, and the bridge returns
/// the `result` (or `error`) it got back from the client-owned server.
#[derive(Debug, Deserialize)]
struct BridgeRequest {
    id: u64,
    session_id: String,
    server_id: String,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct BridgeResponse {
    id: u64,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl BridgeResponse {
    fn success(id: u64, result: Value) -> Self {
        Self {
            id,
            success: true,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: u64, error: String) -> Self {
        Self {
            id,
            success: false,
            result: None,
            error: Some(error),
        }
    }
}

struct AcpMcpBridgeInner {
    client_tx: UnboundedSender<ClientOp>,
}

async fn handle_connection(stream: TcpStream, inner: Arc<AcpMcpBridgeInner>) -> anyhow::Result<()> {
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();
    let mut writer = BufWriter::new(write_half);

    while let Some(line) = reader.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let request: BridgeRequest = match serde_json::from_str(&line) {
            Ok(req) => req,
            Err(err) => {
                warn!(error = %err, "acp mcp bridge received malformed request");
                continue;
            }
        };

        let response = inner.handle_request(request).await;
        let response_json = serde_json::to_string(&response)?;
        writer.write_all(response_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    Ok(())
}

impl AcpMcpBridgeInner {
    /// Open a fresh `mcp/connect` for the target ACP-declared server, run
    /// the MCP `initialize` handshake, forward the requested method, then
    /// let the connection drop. Simpler than caching connections across
    /// calls and cheap enough for a per-tool-call bridge process.
    async fn handle_request(&self, request: BridgeRequest) -> BridgeResponse {
        let BridgeRequest {
            id,
            session_id,
            server_id,
            method,
            params,
        } = request;

        match self.forward(session_id, server_id, method, params).await {
            Ok(result) => BridgeResponse::success(id, result),
            Err(err) => BridgeResponse::error(id, err),
        }
    }

    async fn forward(
        &self,
        _session_id: String,
        server_id: String,
        method: String,
        params: Value,
    ) -> Result<Value, String> {
        let connection_id = self
            .connect_mcp(McpServerAcpId::new(server_id))
            .await?;

        let mut initialize_params = Map::new();
        initialize_params.insert("protocolVersion".into(), json!("2025-11-25"));
        initialize_params.insert("capabilities".into(), json!({}));
        initialize_params.insert(
            "clientInfo".into(),
            json!({"name": "codex-acp-mcp-bridge", "version": env!("CARGO_PKG_VERSION")}),
        );
        self.message_mcp(connection_id.clone(), "initialize", initialize_params)
            .await?;

        let params_map = match params {
            Value::Object(map) => map,
            Value::Null => Map::new(),
            other => {
                let mut map = Map::new();
                map.insert("value".into(), other);
                map
            }
        };
        self.message_mcp(connection_id, &method, params_map).await
    }

    async fn connect_mcp(&self, server_id: McpServerAcpId) -> Result<McpConnectionId, String> {
        let (tx, rx) = oneshot::channel();
        let request = ConnectMcpRequest::new(server_id);
        self.client_tx
            .send(ClientOp::ConnectMcp {
                request,
                response_tx: tx,
            })
            .map_err(|_| "client connect_mcp channel closed".to_string())?;

        match rx.await {
            Ok(Ok(resp)) => Ok(resp.connection_id),
            Ok(Err(err)) => Err(err.message),
            Err(_) => Err("client connect_mcp response dropped".to_string()),
        }
    }

    async fn message_mcp(
        &self,
        connection_id: McpConnectionId,
        method: &str,
        params: Map<String, Value>,
    ) -> Result<Value, String> {
        let request = MessageMcpRequest::new(connection_id, method).params(params);
        let (tx, rx) = oneshot::channel();
        self.client_tx
            .send(ClientOp::MessageMcp {
                request,
                response_tx: tx,
            })
            .map_err(|_| "client message_mcp channel closed".to_string())?;

        match rx.await {
            Ok(Ok(resp)) => serde_json::from_str(resp.0.get()).map_err(|err| err.to_string()),
            Ok(Err(err)) => Err(err.message),
            Err(_) => Err("client message_mcp response dropped".to_string()),
        }
    }
}
