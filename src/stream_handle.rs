use crate::DynError;
use bytes::Bytes;
use futures_util::{ready, Sink, Stream};
use quinn::{RecvStream, SendStream};
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::pin;
use tokio::sync::Mutex;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use y_sync::net::BroadcastGroup;
use y_sync::sync::DefaultProtocol;

#[async_trait::async_trait]
pub trait StreamHandle: Send + Sync + Unpin {
    async fn handle(&self, sender: SendStream, receiver: RecvStream) -> Result<(), DynError>;
}

pub struct AwarenessHandle {
    id: Arc<str>,
    bcast: BroadcastGroup,
}

impl AwarenessHandle {
    pub fn new<S: Into<Arc<str>>>(id: S, bcast: BroadcastGroup) -> Self {
        AwarenessHandle {
            id: id.into(),
            bcast,
        }
    }

    pub fn broadcast_group(&self) -> &BroadcastGroup {
        &self.bcast
    }
}

#[async_trait::async_trait]
impl StreamHandle for AwarenessHandle {
    async fn handle(
        &self,
        sender: SendStream,
        receiver: RecvStream,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let sender = AwarenessSink::from(sender);
        let receiver = AwarenessStream::from(receiver);
        let subscription =
            self.bcast
                .subscribe_with(Arc::new(Mutex::new(sender)), receiver, DefaultProtocol);

        tracing::debug!("{} subscribed new endpoint to awareness updates", self.id);
        Ok(subscription.completed().await?)
    }
}

impl Debug for AwarenessHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AwarenessHandle(`{}`)", self.id)
    }
}

#[derive(Debug)]
pub struct AwarenessSink(FramedWrite<SendStream, LengthDelimitedCodec>);

impl AwarenessSink {
    fn new(stream: SendStream) -> Self {
        let framed = FramedWrite::new(stream, LengthDelimitedCodec::new());
        AwarenessSink(framed)
    }
}

impl From<SendStream> for AwarenessSink {
    fn from(value: SendStream) -> Self {
        Self::new(value)
    }
}

impl Sink<Vec<u8>> for AwarenessSink {
    type Error = crate::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let pinned = unsafe { Pin::new_unchecked(&mut self.0) };
        let result = ready!(pinned.poll_ready(cx));
        match result {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        let pinned = unsafe { Pin::new_unchecked(&mut self.0) };
        pinned.start_send(Bytes::from(item))?;
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let pinned = unsafe { Pin::new_unchecked(&mut self.0) };
        let result = ready!(pinned.poll_flush(cx));
        match result {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let pinned = unsafe { Pin::new_unchecked(&mut self.0) };
        let result = ready!(pinned.poll_close(cx));
        match result {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }
}

#[derive(Debug)]
pub struct AwarenessStream(FramedRead<RecvStream, LengthDelimitedCodec>);

impl AwarenessStream {
    fn new(stream: RecvStream) -> Self {
        let framed = FramedRead::new(stream, LengthDelimitedCodec::new());
        AwarenessStream(framed)
    }
}

impl Stream for AwarenessStream {
    type Item = Result<Vec<u8>, crate::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pinned = unsafe { Pin::new_unchecked(&mut self.0) };
        let maybe_result = ready!(pinned.poll_next(cx));
        match maybe_result {
            None => Poll::Ready(None),
            Some(Ok(data)) => Poll::Ready(Some(Ok(data.to_vec()))),
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
        }
    }
}

impl From<RecvStream> for AwarenessStream {
    fn from(value: RecvStream) -> Self {
        Self::new(value)
    }
}
