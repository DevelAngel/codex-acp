use agent_client_protocol::{Agent, ConnectTo, Error};
use agent_client_protocol::schema::v1::{
    AuthenticateRequest, CancelNotification, InitializeRequest, LoadSessionRequest,
    NewSessionRequest, PromptRequest, SessionNotification, SetSessionModeRequest,
};
use agent_client_protocol::{on_receive_notification, on_receive_request};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;

use std::sync::Arc;

// Submodules
mod commands;
mod config_builder;
mod core;
mod events;
mod prompt;
mod session_manager;
mod utils;

use tracing::warn;
// Public exports
pub use core::{ClientOp, CodexAgent};
pub use session_manager::SessionManager;

impl CodexAgent {
    pub async fn serve(self, transport: impl ConnectTo<Agent>, mut rx: UnboundedReceiver<(SessionNotification, Sender<()>)>, mut client_rx: UnboundedReceiver<ClientOp>) -> Result<(), Error> {
        let agent = Arc::new(self);

        Agent.builder()
            .on_receive_request({
                let agent = agent.clone();
                async move |args: InitializeRequest, responder, _cx| {
                    responder.respond_with_result(agent.initialize(args).await)
                }
            }, on_receive_request!())
            .on_receive_request({
                let agent = agent.clone();
                async move |args: AuthenticateRequest, responder, _cx| {
                    responder.respond_with_result(agent.authenticate(args).await)
                }
            }, on_receive_request!())
            .on_receive_request({
                let agent = agent.clone();
                async move |args: NewSessionRequest, responder, _cx| {
                    tracing::info!(mcp_server_count = args.mcp_servers.len(), "Dispatching new session request"); responder.respond_with_result(agent.new_session(args).await)
                }
            }, on_receive_request!())
            .on_receive_request({
                let agent = agent.clone();
                async move |args: LoadSessionRequest, responder, _cx| {
                    responder.respond_with_result(agent.load_session(args).await)
                }
            }, on_receive_request!())
            .on_receive_request({
                let agent = agent.clone();
                async move |args: SetSessionModeRequest, responder, _cx| {
                    responder.respond_with_result(agent.set_session_mode(args).await)
                }
            }, on_receive_request!())
            .on_receive_request({
                let agent = agent.clone();
                async move |args: PromptRequest, responder, _cx| {
                    responder.respond_with_result(agent.prompt(args).await)
                }
            }, on_receive_request!())
            .on_receive_notification({
                let agent = agent.clone();
                async move |args: CancelNotification, _cx| agent.cancel(args).await
            }, on_receive_notification!())
            .with_spawned(move |conn| async move {
                loop {
                    tokio::select! {
                        msg = rx.recv() => {
                            match msg {
                                Some((notification, tx)) => {
                                    let result = conn.send_notification(notification);
                                    if result.is_err() {
                                        break;
                                    }
                                    let _ = tx.send(());
                                }
                                None => break,
                            }
                        }
                        op = client_rx.recv() => {
                            match op {
                                Some(ClientOp::RequestPermission { request, response_tx }) => {
                                    let _ = response_tx.send(conn.send_request(request).block_task().await);
                                }
                                Some(ClientOp::ReadTextFile { mut request, response_tx }) => {
                                    match agent.session_manager.resolve_acp_session_id(&request.session_id) {
                                        Some(session_id) => {
                                            request.session_id = session_id;
                                            let _ = response_tx.send(conn.send_request(request).block_task().await);
                                        }
                                        None => {
                                            let _ = response_tx.send(Err(Error::invalid_params()
                                                .data("unknown session for read_text_file")));
                                        }
                                    }
                                }
                                Some(ClientOp::WriteTextFile { mut request, response_tx }) => {
                                    match agent.session_manager.resolve_acp_session_id(&request.session_id) {
                                        Some(session_id) => {
                                            request.session_id = session_id.clone();
                                            if agent.session_manager.is_read_only(&session_id) {
                                                let _ = response_tx.send(Err(Error::invalid_params()
                                                    .data("write_text_file is disabled while session mode is read-only")));
                                            } else {
                                                let _ = response_tx.send(conn.send_request(request).block_task().await);
                                            }
                                        }
                                        None => {
                                            let _ = response_tx.send(Err(Error::invalid_params()
                                                .data("unknown session for write_text_file")));
                                        }
                                    }
                                }
                                Some(ClientOp::ConnectMcp { request, response_tx }) => {
                                    let _ = response_tx.send(conn.send_request(request).block_task().await);
                                }
                                Some(ClientOp::MessageMcp { request, response_tx }) => {
                                    let _ = response_tx.send(conn.send_request(request).block_task().await);
                                }
                                Some(ClientOp::MessageMcpNotification { notification }) => {
                                    if let Err(err) = conn.send_notification(notification) {
                                        warn!(?err, "failed to send MCP notification to client");
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                }
                Ok::<(), Error>(())
            })
            .connect_to(transport)
            .await
    }
}
