#![no_std]
#![allow(unused_imports)]

extern crate alloc;

use alloc::{format, string::String, vec::Vec};
use alloc::vec;
use ipc::ipc_message::{IpcMessage, IpcMessageContent, IpcMessageData, IpcMessageType, IpcMessageWrapper};
use ipc::ipc_error::IpcError;
use runtime::*;
use terminal::println;

#[unsafe(no_mangle)]
pub fn main() {

    let args: Vec<String> = env::args().collect();

    let mut args_single = Vec::new();
    let mut args_flags = Vec::new();
    
    let mut idx = 1;
    'outer: loop {
        if idx >= args.len() { break 'outer; }
        let arg = &args[idx];
        if arg.starts_with("-") {

            let flag = &arg[1..];

            // this flag is last arg, break outer loop
            if idx == args.len() - 1 {
                args_flags.push((flag, None));
                break 'outer;
            }

            // check possible value for flag
            idx += 1;
            let value = &args[idx];
            
            if value.starts_with("-") {
                // we found a new flag, decrement idx again
                idx -= 1;
                args_flags.push((flag, None));
            } else {
                args_flags.push((flag, Some(value.as_str())));
            }
        } else {
            // add single arg (without corresponding flag)
            args_single.push(arg.as_str());
        }
        idx += 1;
    }

    if args.len() < 3 {
        return;
    }

    let Some(service_name) = args_flags.iter()
        .find(|(flag, _)| *flag == "service" || *flag == "svc" || *flag == "s")
        .map(|(_, value)| *value)
        .flatten()
        else {
            return; // without service name we cannot send anything
        };

    let msg_name = args_flags.iter()
        .find(|(flag, _)| *flag == "name" || *flag == "n")
        .map(|(_, value)| *value)
        .flatten()
        .unwrap_or("");

    let msg_data_type = args_flags.iter()
        .find(|(flag, _)| *flag == "type" || *flag == "t")
        .map(|(_, value)| *value)
        .flatten()
        .map(|s| s.into())
        .unwrap_or(IpcMessageType::Raw);

    let timeout = args_flags.iter()
        .find(|(flag, _)| *flag == "timeout")
        .map(|(_, value)| *value)
        .flatten()
        .map(|s| s.parse().ok())
        .flatten()
        .unwrap_or(0);
    
    match msg_data_type {
        IpcMessageType::Usize | IpcMessageType::UsizeSlice => {
            let content: Vec<usize> = args_single.iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            send(service_name, msg_name, content.as_slice(), timeout);
        },
        IpcMessageType::Isize | IpcMessageType::IsizeSlice => {
            let content: Vec<isize> = args_single.iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            send(service_name, msg_name, content.as_slice(), timeout);
        },
        IpcMessageType::I32 | IpcMessageType::I32Slice => {
            let content: Vec<i32> = args_single.iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            send(service_name, msg_name, content.as_slice(), timeout);
        },
        IpcMessageType::F64 | IpcMessageType::F64Slice => {
            let content: Vec<f64> = args_single.iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            send(service_name, msg_name, content.as_slice(), timeout);
        },
        IpcMessageType::Empty => {
            send::<&[u8]>(service_name, msg_name, &[], timeout);
        },
        IpcMessageType::Raw => {
            let parsed_floats: Vec<f64> = args_single.iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            let parsed_integers: Vec<isize> = parsed_floats.iter().map(|f| *f as _).collect();
            let bytes = unsafe { core::slice::from_raw_parts(parsed_integers.as_ptr() as *const u8, parsed_integers.len() * size_of::<isize>()) };
            let content = &bytes[..core::cmp::min(bytes.len(), ipc::MESSAGE_DATA_SIZE)];
            send(service_name, msg_name, content, timeout);
        },
        IpcMessageType::String => {
            let content = *args_single.first().unwrap_or(&"");
            send(service_name, msg_name, content, timeout);
        },
        IpcMessageType::Struct => {
            unimplemented!("how would structs work with rust? (missing runtime reflection)");
        },
        _ => {
            send::<&[u8]>(service_name, msg_name, &[], timeout);
        },
    };
}

fn send<T: IpcMessageContent>(service_name: &str, msg_name: &str, content: T, timeout: usize) {
    if timeout > 0 {
        let res = ipc::send_and_receive_busy_polling(service_name, msg_name, content, timeout);
        match res {
            Ok(msg_wrapper) => println!("{:?}", msg_wrapper.msg()),
            Err(err) => println!("{:?}", err),
        };
    } else {
        let res = ipc::send(service_name, msg_name, content);
        match res {
            Ok(_) => {
                println!("msg sent successfully");
            },
            Err(err) => println!("{:?}", err),
        };
    }
}