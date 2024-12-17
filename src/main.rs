#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use std::thread::sleep;
use ipc_channel::ipc;
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (server, server_name) = IpcOneShotServer::new().expect("Failed to create IpcOneShotServer");
    child_opts.ipc_path = server_name.clone();
    //debug server_name
    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    // TODO
    //let (parent_receiver, child_sender) = server.accept().unwrap();
    let (_receiver, (parent_sender, parent_receiver)) = server.accept().expect("Failed to accept connection from child");
    
    (child, parent_sender, parent_receiver)
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    // TODO
    let server_name = opts.ipc_path.clone();

    let bootstrap: Sender<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)> = Sender::connect(server_name.clone()).expect("Failed to connect to coordinator");

    let (to_child, from_parent) = ipc::channel().unwrap();
    let (to_parent, from_child) = ipc::channel().unwrap();
    bootstrap.send((to_child, from_child));
    (to_parent, from_parent)
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");
    // TODO
    let mut coordinator = coordinator::Coordinator::new(coord_log_path, &running);
    
    for client_id in 0..opts.num_clients {
        let mut child_opts = opts.clone();
        child_opts.mode = "client".to_string();
        child_opts.num = client_id.clone();
        let (child, sender, receiver) = spawn_child_and_connect(&mut child_opts);
        coordinator.client_join(&client_id.to_string(), sender, receiver);
    }
    
    for participant_id in 0..opts.num_participants {
        let mut child_opts = opts.clone();
        child_opts.mode = "participant".to_string();
        child_opts.num = participant_id.clone();
        let (child, sender, receiver) = spawn_child_and_connect(&mut child_opts);
        coordinator.participant_join(&participant_id.to_string(), sender, receiver);
    }
    
    coordinator.protocol();

}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // TODO
    let (sender, receiver) = connect_to_coordinator(opts);
    let client_id_str = format!("client_{}", opts.num.clone());
    let mut client = client::Client::new(client_id_str, running.clone(), sender, receiver);

    client.protocol(opts.num_requests.clone());

}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num.clone());
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    // TODO
    let (sender, receiver) = connect_to_coordinator(opts);
    let mut participant = participant::Participant::new(participant_id_str, participant_log_path, running.clone(), opts.send_success_probability.clone(), opts.operation_success_probability.clone(), sender, receiver);

    participant.protocol();
    
}

fn main() {
    // Parse CLI arguments

    let opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("CTRL-C!\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}
