use std::net::ToSocketAddrs;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use batch::Batch;
use crossbeam::channel::{unbounded, Receiver, Sender};
use ductile::{connect_channel, connect_channel_with_enc, ChannelReceiver, ChannelSender};
use log::{info, warn};
use meilisearch_types::keys::Key;
use meilisearch_types::tasks::{KindWithContent, Task};
use serde::{Deserialize, Serialize};

pub mod batch;
mod leader;

pub use leader::Leader;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Network issue occured")]
    NetworkIssue,
    #[error("Internal error: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeaderMsg {
    /// A dump to join the cluster
    JoinFromDump(Vec<u8>),
    /// Starts a new batch
    StartBatch { id: u32, batch: Batch },
    /// Tell the follower to commit the update asap
    Commit(u32),
    /// Tell the follower to commit the update asap
    RegisterNewTask { task: Task, update_file: Option<Vec<u8>> },

    /// Tell the follower to commit the update asap
    ApiKeyOperation(ApiKeyOperation),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FollowerMsg {
    // Let the leader knows you're ready to commit
    ReadyToCommit(u32),
    RegisterNewTask(KindWithContent),
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Consistency {
    One,
    Two,
    Quorum,
    #[default]
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ApiKeyOperation {
    Insert(Key),
    Delete(Uuid),
}

impl std::fmt::Display for Consistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Consistency::One => write!(f, "one"),
            Consistency::Two => write!(f, "two"),
            Consistency::Quorum => write!(f, "quorum"),
            Consistency::All => write!(f, "all"),
        }
    }
}

impl FromStr for Consistency {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "one" => Ok(Consistency::One),
            "two" => Ok(Consistency::Two),
            "quorum" => Ok(Consistency::Quorum),
            "all" => Ok(Consistency::All),
            s => Err(format!(
                "Unexpected value `{s}`, expected one of `one`, `two`, `quorum`, `all`"
            )),
        }
    }
}

#[derive(Clone)]
pub enum Cluster {
    Leader(Leader),
    Follower(Follower),
}

#[derive(Clone)]
pub struct Follower {
    sender: ChannelSender<FollowerMsg>,

    get_batch: Receiver<(u32, Batch)>,
    must_commit: Receiver<u32>,
    register_new_task: Receiver<(Task, Option<Vec<u8>>)>,

    api_key_op: Receiver<ApiKeyOperation>,

    batch_id: Arc<RwLock<u32>>,
}

impl Follower {
    pub fn join(leader: impl ToSocketAddrs, master_key: Option<String>) -> (Follower, Vec<u8>) {
        let (sender, receiver) = if let Some(master_key) = master_key {
            let mut enc = [0; 32];
            let master_key = master_key.as_bytes();
            if master_key.len() < 32 {
                warn!("Master key is not secure, use a longer master key (at least 32 bytes long)");
            }
            enc.iter_mut().zip(master_key).for_each(|(enc, mk)| *enc = *mk);
            info!("Connecting with encryption enabled");
            connect_channel_with_enc(leader, &enc).unwrap()
        } else {
            connect_channel(leader).unwrap()
        };

        info!("Connection to the leader established");

        info!("Waiting for the leader to contact us");
        let state = receiver.recv().unwrap();

        let dump = match state {
            LeaderMsg::JoinFromDump(dump) => dump,
            msg => panic!("Received unexpected message {msg:?}"),
        };

        let (get_batch_sender, get_batch_receiver) = unbounded();
        let (must_commit_sender, must_commit_receiver) = unbounded();
        let (register_task_sender, register_task_receiver) = unbounded();
        let (create_api_key_sender, create_api_key_receiver) = unbounded();

        std::thread::spawn(move || {
            Self::router(
                receiver,
                get_batch_sender,
                must_commit_sender,
                register_task_sender,
                create_api_key_sender,
            );
        });

        (
            Follower {
                sender,
                get_batch: get_batch_receiver,
                must_commit: must_commit_receiver,
                register_new_task: register_task_receiver,
                api_key_op: create_api_key_receiver,
                batch_id: Arc::default(),
            },
            dump,
        )
    }

    fn router(
        receiver: ChannelReceiver<LeaderMsg>,
        get_batch: Sender<(u32, Batch)>,
        must_commit: Sender<u32>,
        register_new_task: Sender<(Task, Option<Vec<u8>>)>,
        api_key_op: Sender<ApiKeyOperation>,
    ) {
        loop {
            match receiver.recv().expect("Lost connection to the leader") {
                LeaderMsg::JoinFromDump(_) => {
                    warn!("Received a join from dump msg but I’m already running : ignoring the message")
                }
                LeaderMsg::StartBatch { id, batch } => {
                    info!("Starting to process a new batch");
                    get_batch.send((id, batch)).expect("Lost connection to the main thread")
                }
                LeaderMsg::Commit(id) => {
                    info!("Must commit");
                    must_commit.send(id).expect("Lost connection to the main thread")
                }
                LeaderMsg::RegisterNewTask { task, update_file } => {
                    info!("Registered a new task");
                    register_new_task
                        .send((task, update_file))
                        .expect("Lost connection to the main thread")
                }
                LeaderMsg::ApiKeyOperation(key) => {
                    api_key_op.send(key).expect("Lost connection to the main thread")
                }
            }
        }
    }

    pub fn get_new_batch(&self) -> Batch {
        info!("Get new batch called");
        let (id, batch) = self.get_batch.recv().expect("Lost connection to the leader");
        info!("Got a new batch");
        *self.batch_id.write().unwrap() = id;
        batch
    }

    pub fn ready_to_commit(&self) {
        info!("I'm ready to commit");
        let batch_id = self.batch_id.read().unwrap();

        self.sender.send(FollowerMsg::ReadyToCommit(*batch_id)).unwrap();

        loop {
            let id = self.must_commit.recv().expect("Lost connection to the leader");
            #[allow(clippy::comparison_chain)]
            if id == *batch_id {
                break;
            } else if id > *batch_id {
                panic!("We missed a batch");
            }
        }
        info!("I got the right to commit");
    }

    pub fn get_new_task(&self) -> (Task, Option<Vec<u8>>) {
        self.register_new_task.recv().expect("Lost connection to the leader")
    }

    pub fn api_key_operation(&self) -> ApiKeyOperation {
        info!("Creating a new api key");
        self.api_key_op.recv().expect("Lost connection to the leader")
    }
}
