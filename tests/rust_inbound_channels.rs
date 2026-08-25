use std::time::Duration;

use panetone::channels::{InboundMessage, SignalSubscriber, TelegramPoller};
use panetone::domain::{ChannelBinding, ChannelKind, Route, RouteId};
use panetone::service::InboundIngestor;
use panetone::store::StoreHandle;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use uuid::Uuid;

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

async fn telegram_document_server(
    update_response: String,
    document: Vec<u8>,
) -> (String, tokio::task::JoinHandle<Vec<(String, Vec<u8>)>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let file_response = serde_json::json!({
        "ok": true,
        "result": {"file_path": "documents/remote.md"}
    })
    .to_string();
    let task = tokio::spawn(async move {
        let responses = [
            update_response.into_bytes(),
            file_response.into_bytes(),
            document,
        ];
        let mut requests = Vec::new();
        for response_body in responses {
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
                .unwrap_or(0);
            while received.len() - header_end < content_length {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                received.extend_from_slice(&chunk[..read]);
            }
            requests.push((
                header.lines().next().unwrap().to_owned(),
                received[header_end..header_end + content_length].to_vec(),
            ));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&response_body).await.unwrap();
        }
        requests
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
    let directory = tempdir().unwrap();
    let poller = TelegramPoller::telegram(
        &base,
        "fake-token",
        -1001,
        directory.path(),
        Duration::from_secs(2),
    )
    .unwrap();
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
    let poller = TelegramPoller::telegram(
        &base,
        "fake-token",
        -1001,
        directory.path(),
        Duration::from_secs(2),
    )
    .unwrap();
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
    let directory = tempdir().unwrap();
    let poller = TelegramPoller::telegram(
        &base,
        "fake-token",
        -1001,
        directory.path(),
        Duration::from_secs(2),
    )
    .unwrap();
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
async fn telegram_document_is_downloaded_before_the_update_is_returned() {
    let response = serde_json::json!({
        "ok": true,
        "result": [{
            "update_id": 100,
            "message": {
                "chat": {"id": -1001},
                "message_thread_id": 77,
                "from": {"id": 42, "first_name": "Alice"},
                "document": {
                    "file_id": "remote-file-id",
                    "file_unique_id": "stable-file-id",
                    "file_name": "../plan.md",
                    "mime_type": "text/markdown",
                    "file_size": 16
                }
            }
        }]
    })
    .to_string();
    let contents = b"# Durable plan\n".to_vec();
    let (base, requests) = telegram_document_server(response, contents.clone()).await;
    let directory = tempdir().unwrap();
    let attachment_directory = directory.path().join("attachments");
    let poller = TelegramPoller::telegram(
        &base,
        "fake-token",
        -1001,
        &attachment_directory,
        Duration::from_secs(2),
    )
    .unwrap();

    let batch = poller.poll(100, 0).await.unwrap();
    assert_eq!(batch.next_offset, 101);
    assert_eq!(batch.messages.len(), 1);
    let attachment = attachment_directory.join("100-plan.md");
    assert_eq!(
        batch.messages[0].body,
        format!("[attached text/markdown: {}]", attachment.display())
    );
    assert_eq!(tokio::fs::read(&attachment).await.unwrap(), contents);

    let requests = requests.await.unwrap();
    assert!(requests[0].0.contains("/botfake-token/getUpdates "));
    assert!(requests[1].0.contains("/botfake-token/getFile "));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&requests[1].1).unwrap()["file_id"],
        "remote-file-id"
    );
    assert!(
        requests[2]
            .0
            .contains("/file/botfake-token/documents/remote.md ")
    );
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
    store
        .save_route(
            Route {
                id: RouteId::new(Uuid::new_v4()),
                title: "private".into(),
                channels: vec![ChannelBinding::Signal {
                    group_id: "group-one".into(),
                }],
                agent: None,
            },
            1,
        )
        .await
        .unwrap();
    let ingestor = InboundIngestor::new(store.clone());
    assert!(
        ingestor
            .ingest_signal_once(&mut subscriber, "+15551111", 300)
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
async fn signal_debate_accepts_group_members_and_adds_first_name_only() {
    let directory = tempdir().unwrap();
    let store = StoreHandle::open(directory.path().join("state.sqlite3")).unwrap();
    store
        .save_route(
            Route {
                id: RouteId::new(Uuid::new_v4()),
                title: "debate".into(),
                channels: vec![ChannelBinding::Signal {
                    group_id: "debate-group==".into(),
                }],
                agent: None,
            },
            1,
        )
        .await
        .unwrap();
    store
        .save_route(
            Route {
                id: RouteId::new(Uuid::new_v4()),
                title: "private".into(),
                channels: vec![ChannelBinding::Signal {
                    group_id: "private-group".into(),
                }],
                agent: None,
            },
            1,
        )
        .await
        .unwrap();
    let ingestor = InboundIngestor::new(store.clone());
    let message =
        |external_id: &str, destination: &str, sender_id: &str, sender: &str| InboundMessage {
            channel: ChannelKind::Signal,
            external_id: external_id.into(),
            destination: destination.into(),
            sender_id: Some(sender_id.into()),
            sender: Some(sender.into()),
            reply_to_external_id: None,
            body: "hello".into(),
        };

    assert!(
        ingestor
            .persist_signal(
                message("friend-debate", "debate-group", "friend", "Andrew RM"),
                "owner",
                10,
            )
            .await
            .unwrap()
    );
    assert!(
        !ingestor
            .persist_signal(
                message("friend-private", "private-group", "friend", "Andrew RM"),
                "owner",
                11,
            )
            .await
            .unwrap()
    );
    assert!(
        ingestor
            .persist_signal(
                message("owner-private", "private-group", "owner", "Mihai Cosma"),
                "owner",
                12,
            )
            .await
            .unwrap()
    );
    assert!(
        !ingestor
            .persist_signal(
                message("friend-unknown", "unknown-group", "friend", "Andrew RM"),
                "owner",
                13,
            )
            .await
            .unwrap()
    );

    let pending = store.pending_inbox().await.unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].body, "Andrew says: hello");
    assert_eq!(pending[1].body, "hello");
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
