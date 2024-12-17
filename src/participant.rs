//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;

use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::TryRecvError;
use participant::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
    commit_count: u64,
    abort_count: u64,
    unknown_count: u64,
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {

    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64, tx: Sender<ProtocolMessage>, rx: Receiver<ProtocolMessage>) -> Participant {

        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,
            // TODO
            tx: tx,
            rx: rx,
            commit_count: 0,
            abort_count: 0,
            unknown_count: 0
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
        if x <= self.send_success_prob {
            // TODO: Send success
            let success_pm = ProtocolMessage::instantiate(MessageType::ParticipantVoteCommit, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
            self.log.append(MessageType::ParticipantVoteCommit, pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
            self.tx.send(success_pm).unwrap();
        } else {
            // TODO: Send fail
            let fail_pm = ProtocolMessage::instantiate(MessageType::ParticipantVoteAbort, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
            self.log.append(MessageType::ParticipantVoteAbort, pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
            self.tx.send(fail_pm).unwrap();
        }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///
    pub fn perform_operation(&mut self, request_option: &Option<ProtocolMessage>) -> bool {

        trace!("{}::Performing operation", self.id_str.clone());
        let x: f64 = random();
        if x <= self.operation_success_prob {
            // TODO: Successful operation
            true
        } else {
            // TODO: Failed operation
            false
        }
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

        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO
        while self.running.load(Ordering::SeqCst) {
            // do nothing, wait for the running flag to be set to false
        }
        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        trace!("{}::Beginning protocol", self.id_str.clone());

        // TODO
        let mut finished = false;
        while self.running.load(Ordering::SeqCst) {
            let pm = self.rx.recv().unwrap();
            match pm.mtype {
                MessageType::CoordinatorPropose => {
                    self.send(pm.clone());
                }
                MessageType::CoordinatorAbort => {
                    // what should i do if the coordinator aborts?
                    self.abort_count = self.abort_count + 1;
                }
                MessageType::CoordinatorCommit => {
                    // do i use the perform_operation function here?
                    if self.perform_operation(&Some(pm.clone())) {
                        //send something to the coordinator
                        let message = ProtocolMessage::instantiate(MessageType::ParticipantVoteCommit, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                        self.tx.send(message).unwrap();

                    }else{
                        //send something to the coordinator
                        let message = ProtocolMessage::instantiate(MessageType::ParticipantVoteAbort, pm.uid.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                        self.tx.send(message).unwrap();
                    }
                }
                MessageType::ClientResultCommit => {
                    self.commit_count +=1;
                    self.log.append(MessageType::CoordinatorCommit, pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                }
                MessageType::ClientResultAbort => {
                    self.abort_count +=1;
                    self.log.append(MessageType::CoordinatorAbort, pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
                }
                MessageType::CoordinatorExit => {
                    finished = true;
                    break;
                }
                _=> {
                
                }
            }
        }
        if !finished {
            self.wait_for_exit_signal();
        }
        self.report_status();
    }
}
