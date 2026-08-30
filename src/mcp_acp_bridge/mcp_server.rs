//! Stdio MCP server spawned per session for one `McpServer::Acp` entry.
//!
//! Codex talks normal MCP to this process. Every `tools/list`/`tools/call`
//! it receives is forwarded verbatim to the [`super::bridge::AcpMcpBridge`]
//! TCP endpoint, which relays it through the ACP connection to the client
//! that actually owns the target MCP server (see `ACP_MCP_*` env vars).

use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result};
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestParam, CallToolResult, Content, ListToolsResult, PaginatedRequestParam,
        ServerCapabilities, ServerInfo, Tool,
    },
    service::{self, RequestContext},
    transport::io,
};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::{Duration, timeout},
};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub async fn run() -> Result<()> {
    crate::init_from_env()?;

    let bridge_addr = env::var("ACP_MCP_BRIDGE_ADDR")
        .context("ACP_MCP_BRIDGE_ADDR environment variable is required")?;
    let session_id = env::var("ACP_MCP_SESSION_ID")
        .context("ACP_MCP_SESSION_ID environment variable is required")?;
    let server_id = env::var("ACP_MCP_SERVER_ID")
        .context("ACP_MCP_SERVER_ID environment variable is required")?;
    let server_name = env::var("ACP_MCP_SERVER_NAME").unwrap_or_else(|_| server_id.clone());

    let server = AcpMcpProxy {
        bridge_addr,
        session_id,
        server_id,
        server_name,
    };
    let transport = io::stdio();
    let running = service::serve_server(server, transport).await?;
    let _ = running.waiting().await;
    Ok(())
}

#[derive(Clone)]
struct AcpMcpProxy {
    bridge_addr: String,
    session_id: String,
    server_id: String,
    server_name: String,
}

impl ServerHandler for AcpMcpProxy {
    fn get_info(&self) -> ServerInfo {
        let caps = ServerCapabilities::builder().enable_tools().build();
        ServerInfo {
            protocol_version: Default::default(),
            capabilities: caps,
            server_info: rmcp::model::Implementation {
                name: format!("codex-acp-mcp-bridge:{}", self.server_name),
                title: Some(format!("Codex ACP MCP Bridge ({})", self.server_name)),
                version: env!("CARGO_PKG_VERSION").into(),
                icons: None,
                website_url: None,
            },
            instructions: None,
        }
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let result = self
            .forward("tools/list", Map::new())
            .await
            .map_err(bridge_error)?;

        let tools: Vec<Tool> = result
            .get("tools")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| McpError::internal_error("malformed tools/list result", Some(json!({"reason": e.to_string()}))))?
            .unwrap_or_default();

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let mut params = Map::new();
        params.insert("name".into(), json!(request.name));
        params.insert(
            "arguments".into(),
            request.arguments.map(Value::Object).unwrap_or(Value::Null),
        );

        let result = self
            .forward("tools/call", params)
            .await
            .map_err(bridge_error)?;

        serde_json::from_value(result).or_else(|_| {
            Ok(CallToolResult::success(vec![Content::text(
                "tool call forwarded, but the response could not be parsed as a standard MCP result",
            )]))
        })
    }
}

fn bridge_error(reason: String) -> McpError {
    McpError::internal_error("acp mcp bridge request failed", Some(json!({ "reason": reason })))
}

impl AcpMcpProxy {
    async fn forward(&self, method: &str, params: Map<String, Value>) -> Result<Value, String> {
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let mut stream = TcpStream::connect(&self.bridge_addr)
            .await
            .map_err(|e| format!("failed to connect to acp mcp bridge: {e}"))?;
        let (reader_half, mut writer_half) = stream.split();
        let mut reader = BufReader::new(reader_half).lines();

        let payload = serde_json::to_string(&json!({
            "id": request_id,
            "session_id": self.session_id,
            "server_id": self.server_id,
            "method": method,
            "params": params,
        }))
        .map_err(|e| e.to_string())?;

        writer_half
            .write_all(payload.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        writer_half.write_all(b"\n").await.map_err(|e| e.to_string())?;
        writer_half.flush().await.map_err(|e| e.to_string())?;

        let line = timeout(Duration::from_secs(30), reader.next_line())
            .await
            .map_err(|_| "acp mcp bridge request timed out".to_string())?
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "acp mcp bridge closed connection".to_string())?;

        let response: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        let success = response
            .get("success")
            .and_then(|s| s.as_bool())
            .unwrap_or(false);

        if success {
            Ok(response.get("result").cloned().unwrap_or(Value::Null))
        } else {
            Err(response
                .get("error")
                .and_then(|e| e.as_str())
                .map(str::to_owned)
                .unwrap_or_else(|| "acp mcp bridge error".to_string()))
        }
    }
}
