use arc_core::client::{Client, Error, TurnEvent};
use tokio::sync::mpsc;

use crate::app::{Command, NetEvent, ReviewEntry};

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
        Command::ReviewList { since_micros } => {
            review_list(&mut client, since_micros, events).await
        }
        Command::ReviewAccept { record_id } => {
            verdict(client.review_accept(&record_id).await, events)
        }
        Command::ReviewDelete { record_id } => {
            verdict(client.review_delete(&record_id).await, events)
        }
        Command::ListJobs => list_jobs(&mut client, events).await,
    };
    match result {
        Ok(()) => Some(client),
        Err(error) => {
            let _ = events.send(NetEvent::Disconnected {
                reason: error.to_string(),
            });
            // dropping the client forces a reconnect on the next command
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
        Ok(answer) => {
            let _ = events.send(NetEvent::History {
                session_id: session_id.to_owned(),
                entries: answer.entries,
            });
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn review_list(
    client: &mut Client,
    since_micros: i64,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.review_items(since_micros).await {
        Ok(items) => {
            let entries = items
                .into_iter()
                .filter_map(|item| {
                    let record = item.record?;
                    Some(ReviewEntry {
                        id: record.id,
                        kind: record.kind,
                        namespace: record.namespace,
                        title: record.title,
                        summary: record.summary,
                        body: record.body,
                        superseded: !item.superseded_by.is_empty(),
                    })
                })
                .collect();
            let _ = events.send(NetEvent::ReviewItems(entries));
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn list_jobs(
    client: &mut Client,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match client.jobs().await {
        Ok(jobs) => {
            let _ = events.send(NetEvent::JobItems(jobs));
            Ok(())
        }
        Err(Error::Server { code, msg }) => {
            let _ = events.send(NetEvent::Failed { code, msg });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn verdict(
    result: Result<(), Error>,
    events: &mpsc::UnboundedSender<NetEvent>,
) -> Result<(), Error> {
    match result {
        Ok(()) => Ok(()),
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
