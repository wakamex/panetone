use std::time::Duration;

use panetone::channels::{SignalSubscriber, TelegramPoller};
use panetone::service::InboundIngestor;
use panetone::store::StoreHandle;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};

async fn telegram_server(
    response_body: &str,
) -> (String, tokio::task::JoinHandle<serde_json::Value>) {
    telegram_server_response("200 OK", response_body).await
}

async fn telegram_server_response(
    status: &str,
    response_body: &str,
) -> (String, tokio::task::JoinHandle<serde_json::Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let status = status.to_owned();
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
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        serde_json::from_slice(&received[header_end..header_end + content_length]).unwrap()
    });
    (format!("http://{address}"), task)
}

#[tokio::test]
async fn telegram_polling_classifies_rate_limits_and_server_errors_as_retryable() {
    let response = serde_json::json!({
        "ok": false,
        "error_code": 429,
        "description": "Too Many Requests",
        "parameters": {"retry_after": 5}
    })
    .to_string();
    let (base, request) = telegram_server_response("429 Too Many Requests", &response).await;
    let poller =
        TelegramPoller::telegram(&base, "fake-token", -1001, Duration::from_secs(2)).unwrap();
    let error = poller.poll(10, 0).await.unwrap_err();
    assert!(matches!(
        error,
        panetone::channels::ChannelDeliveryError::RateLimited {
            retry_after_secs: 5,
            ..
        }
    ));
    assert!(error.retryable());
    request.await.unwrap();

    let response = serde_json::json!({
        "ok": false,
        "error_code": 502,
        "description": "Bad Gateway"
    })
    .to_string();
    let (base, request) = telegram_server_response("502 Bad Gateway", &response).await;
    let poller =
        TelegramPoller::telegram(&base, "fake-token", -1001, Duration::from_secs(2)).unwrap();
    let error = poller.poll(10, 0).await.unwrap_err();
    assert_eq!(
        error,
        panetone::channels::ChannelDeliveryError::Transport(
            panetone::domain::ChannelKind::Telegram
        )
    );
    assert!(error.retryable());
    request.await.unwrap();
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
                    "reply_to_message": {"message_id": 501},
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
    assert_eq!(
        batch.messages[0].reply_to_external_id.as_deref(),
        Some("501")
    );

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
    assert_eq!(
        store.pending_inbox().await.unwrap()[0]
            .reply_to_external_id
            .as_deref(),
        Some("501")
    );
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
                            "attachments": [
                                {"id": "photo.jpg", "contentType": "image/jpeg"}
                            ],
                            "quote": {"id": 8001},
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
    let item = &store.pending_inbox().await.unwrap()[0];
    assert_eq!(item.reply_to_external_id.as_deref(), Some("8001"));
    assert!(
        item.body
            .starts_with("signal hello\n[attached image/jpeg: ")
    );
    assert!(item.body.ends_with("/signal-cli/attachments/photo.jpg]"));
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn signal_attachment_only_notifications_are_delivered() {
    let directory = tempdir().unwrap();
    let socket_path = directory.path().join("signal-attachment.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut request = String::new();
        reader.read_line(&mut request).await.unwrap();
        reader
            .get_mut()
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"panetone-subscribe\",\"result\":{}}\n")
            .await
            .unwrap();
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {"result": {"envelope": {
                "sourceNumber": "+15551111",
                "timestamp": 9003,
                "dataMessage": {
                    "attachments": [
                        {"id": "first.jpg", "contentType": "image/jpeg"},
                        {"id": "second.png", "contentType": "image/png"}
                    ],
                    "groupInfo": {"groupId": "group-one=="}
                }
            }}}
        });
        reader
            .get_mut()
            .write_all(format!("{notification}\n").as_bytes())
            .await
            .unwrap();
    });

    let mut subscriber =
        SignalSubscriber::connect(&socket_path, "+15550000", Duration::from_secs(2))
            .await
            .unwrap();
    let message = subscriber.next().await.unwrap();
    assert!(message.body.contains("[attached image/jpeg: "));
    assert!(message.body.contains("/signal-cli/attachments/first.jpg]"));
    assert!(message.body.contains("[attached image/png: "));
    assert!(
        message
            .body
            .ends_with("/signal-cli/attachments/second.png]")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn signal_receive_deadlines_are_idle_polls() {
    let directory = tempdir().unwrap();
    let socket_path = directory.path().join("signal-idle.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut request = String::new();
        reader.read_line(&mut request).await.unwrap();
        reader
            .get_mut()
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"panetone-subscribe\",\"result\":{}}\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(75)).await;
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "receive",
            "params": {"result": {"envelope": {
                "sourceNumber": "+15551111",
                "timestamp": 9002,
                "dataMessage": {
                    "message": "after idle",
                    "groupInfo": {"groupId": "group-one=="}
                }
            }}}
        });
        reader
            .get_mut()
            .write_all(format!("{notification}\n").as_bytes())
            .await
            .unwrap();
    });

    let mut subscriber =
        SignalSubscriber::connect(&socket_path, "+15550000", Duration::from_millis(20))
            .await
            .unwrap();
    let message = subscriber.next().await.unwrap();
    assert_eq!(message.body, "after idle");
    server.await.unwrap();
}
