use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use panetone::channels::{SignalSubscriber, SlackSocket, TelegramPoller};
use panetone::service::InboundIngestor;
use panetone::store::StoreHandle;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

async fn telegram_server(
    response_body: &str,
) -> (String, tokio::task::JoinHandle<serde_json::Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = response_body.to_owned();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let header_end = loop {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            received.extend_from_slice(&chunk[..read]);
            if let Some(position) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let header = String::from_utf8(received[..header_end].to_vec()).unwrap();
        let content_length = header
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap();
        while received.len() - header_end < content_length {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            received.extend_from_slice(&chunk[..read]);
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        serde_json::from_slice(&received[header_end..header_end + content_length]).unwrap()
    });
    (format!("http://{address}"), task)
}

#[tokio::test]
async fn telegram_updates_are_durable_before_the_confirmation_cursor_advances() {
    let response = serde_json::json!({
        "ok": true,
        "result": [
            {
                "update_id": 10,
                "message": {
                    "chat": {"id": -1001},
                    "message_thread_id": 77,
                    "from": {"id": 42, "first_name": "Alice", "last_name": "Smith"},
                    "text": "hello"
                }
            },
            {
                "update_id": 11,
                "message": {
                    "chat": {"id": -9999},
                    "message_thread_id": 88,
                    "text": "wrong chat"
                }
            },
            {"update_id": 12, "message": {"chat": {"id": -1001}}}
        ]
    })
    .to_string();
    let (base, request) = telegram_server(&response).await;
    let poller =
        TelegramPoller::telegram(&base, "fake-token", -1001, Duration::from_secs(2)).unwrap();
    let batch = poller.poll(10, 0).await.unwrap();
    let request = request.await.unwrap();
    assert_eq!(request["offset"], 10);
    assert_eq!(batch.next_offset, 13);
    assert_eq!(batch.messages.len(), 1);
    assert_eq!(batch.messages[0].destination, "77");
    assert_eq!(batch.messages[0].sender_id.as_deref(), Some("42"));
    assert_eq!(batch.messages[0].sender.as_deref(), Some("Alice Smith"));

    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let ingestor = InboundIngestor::new(store.clone());
    assert_eq!(
        ingestor
            .persist_telegram_batch("telegram_offset", batch.clone(), 100)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store.get_metadata("telegram_offset".into()).await.unwrap(),
        Some("13".into())
    );
    assert_eq!(
        ingestor
            .persist_telegram_batch("telegram_offset", batch, 101)
            .await
            .unwrap(),
        0
    );
    assert_eq!(store.status().await.unwrap().pending_inbox, 1);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn slack_ack_is_sent_only_after_the_envelope_is_durable() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let server_store = store.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(
                serde_json::json!({
                    "envelope_id": "envelope-1",
                    "type": "events_api",
                    "payload": {
                        "event": {
                            "type": "message",
                            "channel": "C123",
                            "user": "U456",
                            "text": "review this"
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let acknowledgement = socket.next().await.unwrap().unwrap();
        let acknowledgement: serde_json::Value =
            serde_json::from_str(acknowledgement.to_text().unwrap()).unwrap();
        assert_eq!(acknowledgement["envelope_id"], "envelope-1");
        assert_eq!(server_store.status().await.unwrap().pending_inbox, 1);
    });

    let mut socket = SlackSocket::connect(&format!("ws://{address}"), Duration::from_secs(2))
        .await
        .unwrap();
    let ingestor = InboundIngestor::new(store.clone());
    assert!(ingestor.ingest_slack_once(&mut socket, 200).await.unwrap());
    server.await.unwrap();
    assert_eq!(store.status().await.unwrap().pending_inbox, 1);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn signal_notifications_are_normalized_and_deduplicated_durably() {
    let directory = tempdir().unwrap();
    let socket_path = directory.path().join("signal.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut request = String::new();
        reader.read_line(&mut request).await.unwrap();
        let request: serde_json::Value = serde_json::from_str(&request).unwrap();
        assert_eq!(request["method"], "subscribeReceive");
        reader
            .get_mut()
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"panetone-subscribe\",\"result\":{}}\n")
            .await
            .unwrap();
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {
                "result": {
                    "envelope": {
                        "sourceNumber": "+15551111",
                        "sourceName": "Alice",
                        "timestamp": 9001,
                        "dataMessage": {
                            "message": "signal hello",
                            "groupInfo": {"groupId": "group-one=="}
                        }
                    }
                }
            }
        });
        let line = format!("{notification}\n");
        reader.get_mut().write_all(line.as_bytes()).await.unwrap();
    });

    let mut subscriber =
        SignalSubscriber::connect(&socket_path, "+15550000", Duration::from_secs(2))
            .await
            .unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    let ingestor = InboundIngestor::new(store.clone());
    assert!(
        ingestor
            .ingest_signal_once(&mut subscriber, 300)
            .await
            .unwrap()
    );
    server.await.unwrap();
    assert_eq!(store.status().await.unwrap().pending_inbox, 1);
    store.shutdown().await.unwrap();
}
