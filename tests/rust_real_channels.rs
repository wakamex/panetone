use std::time::Duration;

use panetone::channels::{
    ChannelDeliveryError, RealChannels, SignalClient, SlackClient, TelegramClient,
};
use panetone::domain::{ChannelKind, EffectId, OutboxItem, OutboxState};
use serde_json::Value;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug)]
struct CapturedHttp {
    request_line: String,
    headers: String,
    body: Value,
}

async fn http_server(
    response: &'static [u8],
    delay: Duration,
) -> (String, JoinHandle<CapturedHttp>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let header_end = loop {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0, "client closed before completing HTTP headers");
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
            assert!(read > 0, "client closed before completing HTTP body");
            received.extend_from_slice(&chunk[..read]);
        }
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        stream.write_all(response).await.unwrap();
        let request_line = header.lines().next().unwrap().to_owned();
        let body =
            serde_json::from_slice(&received[header_end..header_end + content_length]).unwrap();
        CapturedHttp {
            request_line,
            headers: header,
            body,
        }
    });
    (format!("http://{address}"), task)
}

fn item(kind: ChannelKind, destination: &str) -> OutboxItem {
    OutboxItem {
        id: EffectId::new(Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap()),
        route_id: None,
        kind,
        destination: destination.into(),
        body: "message café ✓".into(),
        state: OutboxState::Delivering,
        attempts: 1,
        last_error: None,
        external_receipt: None,
    }
}

#[tokio::test]
async fn telegram_sends_the_expected_stable_request() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 39\r\n\r\n{\"ok\":true,\"result\":{\"message_id\":401}}";
    let (base, request) = http_server(response, Duration::ZERO).await;
    let telegram = TelegramClient::new(&base, "fake-token", -1001, Duration::from_secs(2)).unwrap();
    let receipt = telegram
        .send(&item(ChannelKind::Telegram, "77"))
        .await
        .unwrap();
    assert_eq!(receipt.external_id, "401");
    let request = request.await.unwrap();
    assert_eq!(
        request.request_line,
        "POST /botfake-token/sendMessage HTTP/1.1"
    );
    assert_eq!(request.body["chat_id"], -1001);
    assert_eq!(request.body["message_thread_id"], 77);
    assert_eq!(request.body["text"], "message café ✓");
    assert!(request.headers.contains("x-panetone-delivery-id: 11111111"));
}

#[tokio::test]
async fn slack_uses_client_message_id_and_classifies_destinations() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 24\r\n\r\n{\"ok\":true,\"ts\":\"9.100\"}";
    let (base, request) = http_server(response, Duration::ZERO).await;
    let slack = SlackClient::new(&base, "xoxb-secret", Duration::from_secs(2)).unwrap();
    let receipt = slack.send(&item(ChannelKind::Slack, "C123")).await.unwrap();
    assert_eq!(receipt.external_id, "9.100");
    let request = request.await.unwrap();
    assert_eq!(request.request_line, "POST /chat.postMessage HTTP/1.1");
    assert_eq!(request.body["channel"], "C123");
    assert_eq!(
        request.body["client_msg_id"],
        "11111111-1111-4111-8111-111111111111"
    );
    assert!(
        request
            .headers
            .contains("authorization: Bearer xoxb-secret")
    );

    let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 40\r\n\r\n{\"ok\":false,\"error\":\"channel_not_found\"}";
    let (base, request) = http_server(response, Duration::ZERO).await;
    let slack = SlackClient::new(&base, "secret", Duration::from_secs(2)).unwrap();
    assert!(matches!(
        slack.send(&item(ChannelKind::Slack, "gone")).await,
        Err(ChannelDeliveryError::DestinationUnavailable {
            kind: ChannelKind::Slack,
            ..
        })
    ));
    request.await.unwrap();
}

#[tokio::test]
async fn http_channels_classify_rate_limits_malformed_timeouts_and_disconnects() {
    let response = b"HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: 83\r\n\r\n{\"ok\":false,\"error_code\":429,\"description\":\"retry\",\"parameters\":{\"retry_after\":17}}";
    let (base, request) = http_server(response, Duration::ZERO).await;
    let client = TelegramClient::new(&base, "secret", -1, Duration::from_secs(2)).unwrap();
    assert!(matches!(
        client.send(&item(ChannelKind::Telegram, "1")).await,
        Err(ChannelDeliveryError::RateLimited {
            retry_after_secs: 17,
            ..
        })
    ));
    request.await.unwrap();

    let response = b"HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 83\r\n\r\n{\"ok\":false,\"error_code\":400,\"description\":\"Bad Request: message thread not found\"}";
    let (base, request) = http_server(response, Duration::ZERO).await;
    let client = TelegramClient::new(&base, "secret", -1, Duration::from_secs(2)).unwrap();
    assert!(matches!(
        client.send(&item(ChannelKind::Telegram, "1")).await,
        Err(ChannelDeliveryError::DestinationUnavailable {
            kind: ChannelKind::Telegram,
            ..
        })
    ));
    request.await.unwrap();

    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nnope";
    let (base, request) = http_server(response, Duration::ZERO).await;
    let client = TelegramClient::new(&base, "secret", -1, Duration::from_secs(2)).unwrap();
    assert!(matches!(
        client.send(&item(ChannelKind::Telegram, "1")).await,
        Err(ChannelDeliveryError::Malformed(ChannelKind::Telegram))
    ));
    request.await.unwrap();

    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{";
    let (base, request) = http_server(response, Duration::ZERO).await;
    let client = TelegramClient::new(&base, "secret", -1, Duration::from_secs(2)).unwrap();
    let error = client
        .send(&item(ChannelKind::Telegram, "1"))
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ChannelDeliveryError::Transport(ChannelKind::Telegram)
    );
    assert!(error.retryable());
    request.await.unwrap();

    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
    let (base, request) = http_server(response, Duration::from_millis(200)).await;
    let client = TelegramClient::new(&base, "secret", -1, Duration::from_millis(20)).unwrap();
    let error = client
        .send(&item(ChannelKind::Telegram, "1"))
        .await
        .unwrap_err();
    assert_eq!(error, ChannelDeliveryError::Timeout(ChannelKind::Telegram));
    assert!(!error.to_string().contains("secret"));
    let _ = request.await;
}

#[tokio::test]
async fn signal_uses_stable_json_rpc_identity_and_handles_protocol_failures() {
    let directory = tempdir().unwrap();
    let socket = directory.path().join("signal.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut request = String::new();
        reader.read_line(&mut request).await.unwrap();
        let request: Value = serde_json::from_str(&request).unwrap();
        assert_eq!(request["method"], "send");
        assert_eq!(request["id"], "11111111-1111-4111-8111-111111111111");
        assert_eq!(request["params"]["groupId"], "group-one");
        assert_eq!(request["params"]["account"], "+15550000");
        reader
            .get_mut()
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"11111111-1111-4111-8111-111111111111\",\"result\":{\"timestamp\":12345}}\n")
            .await
            .unwrap();
    });
    let signal = SignalClient::new(&socket, "+15550000", Duration::from_secs(2));
    let receipt = signal
        .send(&item(ChannelKind::Signal, "group-one"))
        .await
        .unwrap();
    assert_eq!(receipt.external_id, "12345");
    server.await.unwrap();

    let socket = directory.path().join("closed.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(stream);
    });
    let signal = SignalClient::new(&socket, "+15550000", Duration::from_secs(2));
    let error = signal
        .send(&item(ChannelKind::Signal, "group-one"))
        .await
        .unwrap_err();
    assert_eq!(error, ChannelDeliveryError::Transport(ChannelKind::Signal));
    assert!(error.retryable());
    server.await.unwrap();

    let socket = directory.path().join("rejected.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        stream
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"11111111-1111-4111-8111-111111111111\",\"error\":{\"code\":-1,\"message\":\"group missing\"}}\n")
            .await
            .unwrap();
    });
    let signal = SignalClient::new(&socket, "+15550000", Duration::from_secs(2));
    assert!(matches!(
        signal.send(&item(ChannelKind::Signal, "group-one")).await,
        Err(ChannelDeliveryError::Rejected {
            kind: ChannelKind::Signal,
            ..
        })
    ));
    server.await.unwrap();

    let socket = directory.path().join("timeout.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let signal = SignalClient::new(&socket, "+15550000", Duration::from_millis(20));
    assert_eq!(
        signal
            .send(&item(ChannelKind::Signal, "group-one"))
            .await
            .unwrap_err(),
        ChannelDeliveryError::Timeout(ChannelKind::Signal)
    );
    server.abort();
}

#[tokio::test]
async fn dispatcher_fails_closed_when_a_channel_is_not_configured() {
    let channels = RealChannels::default();
    assert_eq!(
        channels
            .send(&item(ChannelKind::Telegram, "1"))
            .await
            .unwrap_err(),
        ChannelDeliveryError::NotConfigured(ChannelKind::Telegram)
    );
}
