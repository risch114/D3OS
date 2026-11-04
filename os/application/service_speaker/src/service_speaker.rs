#![no_std]

extern crate alloc;

mod speaker;

use alloc::{string::String, vec::Vec};
use alloc::vec;
use concurrent::thread;
use ipc::ipc_message::IpcMessageWrapper;
use runtime::env::args;
#[allow(unused_imports)]
use runtime::*;

#[unsafe(no_mangle)]
pub fn main() {

    let args: Vec<String> = args().collect();

    if args.len() < 2 || args[1] != "0x42" {
        thread::start_application("service_speaker", vec!["0x42"]);
    } else {

        let endpoint_name = "svc_speaker";
        
        let _res = ipc::register_ipc_endpoint(endpoint_name);
    
        loop {
    
            let rec = ipc::receive_blocking(endpoint_name, 5000);
            
            if let Ok(msg_wrapper) = rec {
                let msg = msg_wrapper.msg();
                match msg.name().as_str_unchecked() {
                    "play" | "Play" | "PLAY" => {
                        if let Ok(data) = msg.data_as_usize_slice() {
                            let freq = *data.get(0).unwrap_or(&8);
                            let dura = *data.get(1).unwrap_or(&1000);
                            speaker::play(freq, dura);
                        }
                    },
                    _ => {},
                }
            }
            
            concurrent::thread::sleep(200);
        }
    }
}
