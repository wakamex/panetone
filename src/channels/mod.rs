mod fake;
mod real;

pub use fake::{ChannelCall, ChannelError, RecordingChannels};
pub use real::{
    ChannelDeliveryError, DebateClient, DeliveryReceipt, RealChannels, SignalClient, SlackClient,
    TelegramClient,
};
