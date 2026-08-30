use codex_acp::{AcpMcpBridge, CodexAgent, FsBridge};
use anyhow::Result;
use codex_core::config::Config;
use std::env;
use tokio::{io, sync::mpsc};
use agent_client_protocol::ByteStreams;
use tokio_util::compat::{TokioAsyncReadCompatExt as _, TokioAsyncWriteCompatExt as _};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tokio::task::LocalSet::new()
        .run_until(async {
            codex_acp::init_from_env()?;

            if env::args().nth(1).as_deref() == Some("--acp-fs-mcp") {
                return codex_acp::fs::run_mcp_server().await;
            }

            if env::args().nth(1).as_deref() == Some("--acp-mcp-bridge") {
                return codex_acp::mcp_acp_bridge::run_mcp_server().await;
            }

            let outgoing = io::stdout().compat_write();
            let incoming = io::stdin().compat();
            let transport = ByteStreams::new(outgoing, incoming);
            let (tx, rx) = mpsc::unbounded_channel();
            let (client_tx, client_rx) = mpsc::unbounded_channel();
            let config = Config::load_with_cli_overrides(vec![]).await?;
            let cwd_path = config.cwd.clone();
            let fs_bridge = FsBridge::start(client_tx.clone(), cwd_path).await?;
            let mcp_bridge = AcpMcpBridge::start(client_tx.clone()).await?;
            let agent = CodexAgent::with_config(
                tx,
                client_tx,
                config,
                Some(fs_bridge),
                Some(mcp_bridge),
            );

            agent.serve(transport, rx, client_rx).await?;
            Ok(())
        })
        .await
}

