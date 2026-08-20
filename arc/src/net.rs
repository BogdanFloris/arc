//! The connection task: owns the wire client, runs commands, reports events.
//!
//! The task connects lazily — on the first command, and again on the first
//! command after a failure — so "the daemon was down" is a fault the next
//! send can heal instead of a fatal state. Commands arrive on a channel and
//! run one at a time, which is also the daemon's own concurrency model.

use arc_core::client::{Client, Error, TurnEvent};
use tokio::sync::mpsc;

use crate::app::{Command, NetEvent};

/// Runs commands against `url` until the command channel closes.
pub async fn run(
    url: String,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<NetEvent>,
) {
    let mut client: Option<Client> = None;

    while let Some(command) = commands.recv().await {
        let connected = match client.take() {
            Some(connected) => connected,
            None => match Client::connect(&url).await {
                Ok(connected) => connected,
                Err(error) => {
                    let _ = events.send(NetEvent::Disconnected {
                        reason: error.to_string(),
                    });
                    continue;
                }
            },
        };
        client = handle(connected, command, &events).await;
    }
}

/// Runs one command, returning the client if the connection is still good.
async fn handle(
    mut client: Client,
    command: Command,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Option<Client> {
    let result = match command {
        Command::List => list(&mut client, events).await,
        Command::History { session_id } => history(&mut client, &session_id, events).await,
        Command::Send {
            session_id,
            content,
        } => send(&mut client, session_id.as_deref(), &content, events).await,
    };
    match result {
        Ok(()) => Some(client),
        Err(error) => {
            let _ = events.send(NetEvent::Disconnected {
                reason: error.to_string(),
            });
            None
        }
    }
}

async fn list(client: &mut Client, events: &mpsc::UnboundedSender<NetEvent>) -> Result<(), Error> {
    match client.list_sessions().await {
        Ok(sessions) => {
            let _ = events.send(NetEvent::Sessions(sessions));
            Ok(())
        }
        // The daemon said no to this request; the connection is fine.
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn history(
    client: &mut Client,
    session_id: &str,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.fetch_history(session_id).await {
        Ok(messages) => {
            let _ = events.send(NetEvent::History {
                session_id: session_id.to_owned(),
                messages: messages
                    .into_iter()
                    .map(|message| (message.role, message.content))
                    .collect(),
            });
            Ok(())
        }
        // The daemon said no to this request; the connection is fine.
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn send(
    client: &mut Client,
    session_id: Option<&str>,
    content: &str,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    let mut turn = client.send_message(session_id, content).await?;
    while let Some(event) = turn.next().await? {
        let event = match event {
            TurnEvent::Accepted { session_id } => NetEvent::Accepted { session_id },
            TurnEvent::Delta(text) => NetEvent::Delta(text),
            TurnEvent::Reasoning(text) => NetEvent::Reasoning(text),
            TurnEvent::ToolCallStarted { call_id, name, .. } => {
                NetEvent::ToolStarted { call_id, name }
            }
            TurnEvent::ToolCallEnded { call_id, outcome } => {
                NetEvent::ToolEnded { call_id, outcome }
            }
            TurnEvent::End { partial, .. } => NetEvent::End { partial },
            TurnEvent::Failed { code, msg } => NetEvent::Failed { code, msg },
        };
        let _ = events.send(event);
    }
    Ok(())
}
