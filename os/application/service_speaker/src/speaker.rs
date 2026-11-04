#![no_std]

extern crate alloc;

use alloc::{string::String, vec::Vec};
use alloc::vec;
use concurrent::{process, thread};
use ipc::ipc_message::IpcMessageWrapper;
use runtime::env::args;
#[allow(unused_imports)]
use runtime::*;
use syscall::syscall;

#[unsafe(no_mangle)]
pub fn main() {

    let args: Vec<String> = args().collect();

    if args.len() < 2 || args[1] != "0x42" {
        thread::start_application("service_speaker", vec!["0x42"]);
    } else {

        let endpoint_name = "svc_speaker";
        
        ipc::register_ipc_endpoint(endpoint_name);
    
        loop {
    
            let rec = ipc::receive_blocking(endpoint_name, 5000);
    
            match rec {
                Ok(msg_wrapper) => {
                    let msg = msg_wrapper.msg();
                    match msg.name().as_str_unchecked() {
                        "play" | "Play" | "PLAY" => {
                            if let Ok(data) = msg.data_as_usize_slice() {
                                syscall(syscall::SystemCall::SpeakerPlaySound, &[*data.get(0).unwrap_or(&0), *data.get(1).unwrap_or(&0)]);
                            }
                        },
                        _ => {},
                    }
                },
                Err(_) => {},
            }
            
            concurrent::thread::sleep(200);
        }
    }
}
