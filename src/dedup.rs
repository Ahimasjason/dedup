use std::{collections::HashMap, time::{Duration, Instant}, pin::Pin};

use futures::{Stream, ready};
use rdkafka::{message::BorrowedMessage, error::KafkaError, Message};
use std::task::Poll;
use std::sync::Arc;

type SessionId = String;

struct DedupTransformer<'a, S>
where
    S: Stream<Item = Result<BorrowedMessage<'a>, KafkaError>>,
{
    stream: S,
    //  we can change this state to redis to share between multiple state
    state: HashMap<Instant, HashMap<SessionId, Arc<SessionMessage>>>,
}

impl<'a, S> DedupTransformer<'a, S>
where
    S: Stream<Item = Result<BorrowedMessage<'a>, KafkaError>>,
{
    fn new(stream: S) -> Self {
        Self {
            stream,
            state: HashMap::new(),
        }
    }

    fn check_state(&self,instant: Instant, session_id : &str) -> bool {
        self.state
            .get(&instant)
            .is_some()
                &&
        self.state
            .get(&instant)
            .unwrap()
            .get(session_id).is_some()
    }

    fn add_message(&mut self,instant : Instant, session_id : String, msg : Arc<SessionMessage>){

        self.state.entry(instant)
                .or_insert_with(HashMap::new)
                .insert(session_id, msg);

    }
}

impl<'a, S> Stream for DedupTransformer<'a, S>
where
    S: Stream<Item = Result<BorrowedMessage<'a>, KafkaError>> + Unpin,
{
    type Item = Result<Arc<SessionMessage>, AppError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Poll the inner stream for the next item
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                
                let msg = msg.detach();
                let payload = msg
                                        .payload()
                                        .ok_or_else(||AppError::InvalidBytes)?;
                let deserialized = serde_json::from_slice::<SessionMessage>(payload)
                                                                    .map_err(|err| AppError::InvalidBytes)?;
                
                let five_min_back =Instant::now()  - Duration::from_secs(5 * 60);
                

                if !self.check_state(five_min_back, &deserialized.id) {
                    let msg = Arc::new(deserialized);
                    self.as_mut().add_message(Instant::now(), msg.id.clone(), msg.clone());
                    std::task::Poll::Ready(Some(Ok(msg)))
                } else {
                    // Message should be skipped, poll again for the next one
                    self.as_mut().poll_next(cx)
                }
            }
            _ => {
                todo!();
            }
        }
    }
}

#[derive(serde::Deserialize)]
struct SessionMessage {
    id : String
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("bytes could not form struct")]
    InvalidBytes,

    #[error("duplicate")]
    Duplicate

}