//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

use crate::participant;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision
}

/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    participants: HashMap<String, ParticipantInfo>,
    clients: HashMap<String, ClientInfo>,
    commit_count: u64,
    abort_count: u64,
    unknown_count: u64
}

#[derive(Debug)]
pub struct ParticipantInfo {
    id: String,
    communication_channels: Vec<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
}
#[derive(Debug)]
pub struct ClientInfo {
    id: String,
    communication_channels: Vec<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
}
///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            // TODO
            participants: HashMap::new(),
            clients: HashMap::new(),
            commit_count: 0,
            abort_count: 0,
            unknown_count: 0
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: &String,tx: Sender<ProtocolMessage>, rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        let participant_info = ParticipantInfo{
            id: name.clone(),
            communication_channels: vec![(tx, rx)],
        };

        self.participants.insert(name.clone(), participant_info);
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, name: &String,tx: Sender<ProtocolMessage>, rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        let client_info = ClientInfo{
            id: name.clone(),
            communication_channels: vec![(tx,rx)],
        };

        self.clients.insert(name.clone(), client_info);
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops: u64 = self.commit_count;
        let failed_ops: u64 = self.abort_count;
        let unknown_ops: u64 = self.unknown_count;
        //log stats into logfile

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {

        // TODO
        let timeout_duration = Duration::from_secs(1);
        let mut last_request_time = Instant::now();
        let mut finished = false;
        while self.running.load(Ordering::SeqCst) {
            debug!("at start of while loop");
            if finished {
                break;
            }
            for (client_id, client_info) in &self.clients {
                let (client_tx,client_rx) = &client_info.communication_channels[0];

                match client_rx.try_recv() {
                    Ok(pm) => {
                        self.state = CoordinatorState::ReceivedRequest;
                        last_request_time = Instant::now();
                        for (participant_id, participant_info) in &self.participants {
                            let (p_tx, _) = &participant_info.communication_channels[0];

                            let message = ProtocolMessage::instantiate(MessageType::CoordinatorPropose, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                            p_tx.send(message).unwrap();
                        }
                        //debug!("Coordinator sent all proposals to participants");
                        let mut votes = Vec::new();
                        for (participant_id, participant_info) in &self.participants {
                            let (_,p_rx) = &participant_info.communication_channels[0];

                            let vote = p_rx.recv().unwrap();
                            votes.push(vote);
                        }
                        //debug!("Coordinator received all proposals to participants");
                        if votes.iter().all(|vote| match vote{ProtocolMessage{mtype:MessageType::ParticipantVoteCommit, ..} => true, _ => false}) {
                            self.state = CoordinatorState::ReceivedVotesCommit;
                            for (participant_id, participant_info) in &self.participants {
                                let (p_tx, _) = &participant_info.communication_channels[0];

                                let message = ProtocolMessage::instantiate(MessageType::CoordinatorCommit, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                                
                                p_tx.send(message).unwrap();
                            }
                            //check if any errors with participant operation then send client result commit or client result abort to participant so they know if it committed or aborted to end transaction
                            //TODO
                            //
                            //
                            let mut execution_votes = Vec::new();
                            for (participant_id, participant_info) in &self.participants {
                                let (_, p_rx) = &participant_info.communication_channels[0];

                                let vote = p_rx.recv().unwrap();

                                execution_votes.push(vote);
                            }

                            if execution_votes.iter().all(|vote| match vote{ProtocolMessage{mtype:MessageType::ParticipantVoteCommit, ..} => true, _ => false}){
                                self.commit_count = self.commit_count + 1;
                                for (participant_id, participant_info) in &self.participants {
                                    let (p_tx, _) = &participant_info.communication_channels[0];

                                    let message = ProtocolMessage::instantiate(MessageType::ClientResultCommit, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());

                                    p_tx.send(message).unwrap();
                                }
                                client_tx.send(ProtocolMessage::instantiate(MessageType::ClientResultCommit, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone()));
                                self.log.append(MessageType::CoordinatorCommit, pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                            }else{
                                self.abort_count = self.abort_count + 1;
                                for (participant_id, participant_info) in &self.participants {
                                    let (p_tx, _) = &participant_info.communication_channels[0];

                                    let message = ProtocolMessage::instantiate(MessageType::ClientResultAbort, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                                    
                                    p_tx.send(message).unwrap();
                                }
                                client_tx.send(ProtocolMessage::instantiate(MessageType::ClientResultAbort, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone()));
                                self.log.append(MessageType::CoordinatorAbort, pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                            }

                        }else{
                            self.state = CoordinatorState::ReceivedVotesAbort;
                            for (participant_id, participant_info) in &self.participants {
                                let (p_tx, _) = &participant_info.communication_channels[0];

                                let message = ProtocolMessage::instantiate(MessageType::CoordinatorAbort, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                                
                                p_tx.send(message).unwrap();
                            }
                            self.abort_count = self.abort_count + 1;
                            client_tx.send(ProtocolMessage::instantiate(MessageType::ClientResultAbort, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone()));
                        }

                        self.state = CoordinatorState::Quiescent;

                    }
                    Err(TryRecvError::Empty) => {
                        // No messages received; check for timeout
                        finished = true;
                        break;
                    }
                    _ => {
                        finished = true;
                    }

                }
            }
        }
        self.report_status();
        for (participant_id, participant_info) in &self.participants {
            let (p_tx, _) = &participant_info.communication_channels[0];

            let message = ProtocolMessage::instantiate(MessageType::CoordinatorExit, 0, "".to_string(), "".to_string(), 0);
            p_tx.send(message).unwrap();
        }
    }
}
