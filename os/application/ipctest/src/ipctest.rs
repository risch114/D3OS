#![no_std]
#![allow(unused_imports)]

extern crate alloc;

use core::fmt::Display;
use core::sync::atomic::AtomicBool;

use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::{format, string::String, vec::Vec};
use alloc::vec;
use concurrent::{process, thread};
use d3os::const_string::ConstString;
use ipc::ipc_message::{IpcMessageData, IpcMessageType, IpcMessageWrapper};
use ipc::ipc_error::IpcError;
use runtime::*;
use syscall::syscall;
use terminal::KeyCode::I;
use terminal::println;
use uuid::Uuid;

static LOGGER: logger::Logger = logger::Logger::new();

#[unsafe(no_mangle)]
pub fn main() {
    let process = process::current();
    let thread = thread::current().unwrap();
    let tid = thread.id();

    log::set_logger(&LOGGER).map(|_| log::set_max_level(log::LevelFilter::Debug));
    //log::info!("[{}:{}] Successfully initialized logger!", process.id(), tid);

    println!("Started Thread [{}] in Process [{}]", thread.id(), process.id());

    let mut args: Vec<String> = env::args().collect();

    if args.len() <= 1 {
        args = vec!["ipctest".to_string(), "start".to_string(), "SIMPLE".to_string(), "67".to_string()];
    }

    let command = args.get(1).unwrap();

    if command == "send" {

        if args.len() < 3 {
            return;
        }

        let res = send_msg_from_args(&args[2..]);

        println!("{:?}", res);

    } else if command == "start" {

        let mut args_receive = Vec::new();
        args_receive.push("start_receive");
        thread::start_application("ipctest", args_receive);

        thread::sleep(1_000);

        let mut args_send = Vec::new();
        args_send.push("start_send");
        args.iter().skip(2).for_each(|s| args_send.push(s));
        thread::start_application("ipctest", args_send);

        return;
    } else if command == "start_send" {

        let args_numbers: Vec<_> = args.iter().skip(2)
            .map(|s| str::parse::<usize>(&s))
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap())
            .collect();

        let res = ipc::send("ipctest", "ipctest_recv", "ipctest numbers", args_numbers.as_slice());
        match res {
            Ok(_) => println!("Send message successfully"),
            Err(err) => println!("Error sending message: {:?}", err),
        }
    } else if command == "start_receive" {
        let endpoint = ipc::register_ipc_endpoint("ipctest", "ipctest_recv").unwrap();
        loop {
            match endpoint.receive_ref() {
                Ok(msg_wrapper) => {
                    let m = msg_wrapper.msg();
                    println!("Received Message: {}", m);
                    break;
                },
                Err(err) => {
                    if err != IpcError::Empty {
                        println!("[{}] Error: {:?}", tid, err);
                    }
                },
            }
            thread::sleep(1_000);
        }
    } else if command == "pingpong" {

        let service_name = "pingpong";
        let endpoint_name = "pong";

        // register self as pong
        let endpoint = ipc::register_ipc_endpoint(service_name, endpoint_name).unwrap();

        // start ping
        thread::start_application("ipctest", vec!["pingpong_ping"]);

        // wait for ping
        loop {
            match endpoint.receive_ref_blocking(5000) {
                Ok(msg) => {
                    let ping = msg.msg().data_as_usize_unchecked().unwrap();
                    let pong = ping + 1;
                    log::info!("PONG! [{}]", pong);
                    ipc::answer_with_callback(&msg.msg(), "pong", (service_name, endpoint_name), pong);
                    if ping > 100 {
                        println!("Pong done!");
                        return;
                    }
                },
                Err(err) => {
                    match err {
                        IpcError::Empty => {},
                        _ => log::error!("[{}] Error: {:?}", tid, err),
                    }
                },
            }
        }
    } else if command == "pingpong_ping" {

        let service_name = "pingpong";
        let endpoint_name = "ping";

        // register self as ping
        let endpoint = ipc::register_ipc_endpoint(service_name, endpoint_name).unwrap();

        // send first ping
        log::info!("PING! [{}]", 1);
        ipc::send_with_callback(service_name, "pong", "ping", (service_name, endpoint_name), 1);

        // wait for pong
        loop {
            match endpoint.receive_ref_blocking(5000) {
                Ok(msg) => {
                    let pong = msg.msg().data_as_usize_unchecked().unwrap();
                    let ping = pong + 1;
                    log::info!("PING! [{}]", ping);
                    ipc::answer_with_callback(&msg.msg(), "ping", (service_name, endpoint_name), ping);
                    if pong > 100 {
                        println!("Ping done!");
                        return;
                    }
                },
                Err(err) => {
                    match err {
                        IpcError::Empty => {},
                        _ => log::error!("[{}] Error: {:?}", tid, err),
                    }
                },
            }
        }
    } else if command == "loop" {

        let limit = 50;
        let service_name = "loop";

        let endpoint = ipc::register_ipc_endpoint(service_name, "recv").unwrap();

        thread::start_application("ipctest", vec!["loop_send"]);

        let mut prev = 0;

        loop {
            while let Ok(msg) = endpoint.receive_ref() {
                let number = msg.msg().data_as_usize_unchecked().unwrap();
                println!("Received Message: {}", number);
                if number == limit {
                    println!("Successfully received {} messages. Sending STOP signal and exiting...", limit);
                    ipc::send_empty(service_name, "send", "STOP");
                    return;
                }
                if prev + 1 == number {
                    prev = number;
                } else {
                    println!("Error in message order. Sending STOP signal and exiting...");
                    ipc::send_empty(service_name, "send", "STOP");
                    return;
                }
            }
            thread::sleep(500);
        }
    } else if command == "loop_send" {

        let service_name = "loop";
        let endpoint = ipc::register_ipc_endpoint(service_name, "send").unwrap();

        let mut i = 1 as usize;
        let mut errors = 0;

        loop {

            // check for stop signal
            if let Ok(msg) = endpoint.receive_ref() {

                if msg.msg().name_as_str() == "STOP" {
                    println!("Received STOP signal ({} errors in total), exiting...", errors);
                    break;
                }
            }

            let res = ipc::send(service_name, "recv", "number", i);
            match res {
                Ok(_) => {
                    println!("Send Message: {} with {} errors", i, errors);
                    i += 1;
                },
                //Err(err) => println!("Error sending message: {} [{:?}]", i, err),
                Err(_) => errors += 1,
            }
            thread::sleep(100);
        }
    } else if command == "struct" {
        
        let endpoint = ipc::register_ipc_endpoint("ipctest_struct", "recv").unwrap();
        thread::start_application("ipctest", vec!["struct_send"]);

        thread::sleep(1500);

        loop {
            match endpoint.receive_ref() {
                Ok(msg) => {
                    let my_struct: &MyStruct = unsafe { ipc::bytes_as_any(&*msg.msg().data_as_bytes()) };
                    println!("-- Receive --\n{:}", my_struct);
                    break;
                },
                Err(err) => println!("[{}] Error: {:?}", tid, err),
            }
            thread::sleep(300);
        }

    } else if command == "struct_send" {
        let my_struct = MyStruct {
            a: 42,
            b: -67,
            c: ConstString::from("Hello World!"),
        };
        println!("-- Sender --\n{:}", my_struct);
        ipc::send_struct("ipctest_struct", "recv", "test", &my_struct);
    } else if command == "contention" || command == "c" {

        let service_name = "ipctest_contention";
        
        let endpoint = ipc::register_ipc_endpoint(service_name, "recv").unwrap();

        let num_child_processes = 4;

        thread::create(move || {
            for _ in 0..num_child_processes {
                thread::start_application("ipctest", vec!["contention_sender"]);
            }
        });

        thread::sleep(1000);

        let mut num_stop_req = 0;

        loop {
            match endpoint.receive_ref() {
                Ok(msg_wrapper) => {
                    let msg = msg_wrapper.msg();
                    match msg.name_as_str() {
                        "STOP" => {
                            num_stop_req += 1;
                            log::info!("[{}] received STOP request from {} -- [{}/{}]", tid, msg.sender_tid(), num_stop_req, num_child_processes);
                            if num_stop_req >= num_child_processes {
                                log::info!("[{}] received {} STOP requests, exiting...", tid, num_stop_req);
                                return;
                            }
                        },
                        _ => {
                            if let Ok(data) = msg.data_as_usize_slice() && data.len() >= 2 {
                                log::info!("[{}] received request {} * {} from {}", tid, data[0], data[1], msg.sender_tid());
                                let res = data[0] * data[1];
                                log::info!("[{}] sending answer {} to {}", tid, res, msg.sender_tid());
                                ipc::answer(msg, "answer", res);
                            } else {
                                log::warn!("Invalid msg: {}", msg);
                            }
                        },
                    }
                },
                Err(err) => {
                    if err != IpcError::Empty {
                        log::error!("[{}] Error: {:?}", tid, err);
                    }
                },
            }
            thread::sleep(100);
        }
    } else if command == "contention_sender" {
        let service_name = "ipctest_contention";
        let mut a = tid;
        let b = 2;
        loop {
            log::info!("{} Sending {}*{}", tid, a, b);
            match ipc::send_and_receive_busy_polling(service_name, "recv", "mult", &[a, b], 5000) {
                Ok(msg_wrapper) => {
                    a = msg_wrapper.msg().data_as_usize_unchecked().unwrap();
                    match a {
                        0 => {
                            log::error!("{} Error, received zero!", tid);
                        },
                        5000..usize::MAX => {
                            log::info!("{} Received {}", tid, a);
                            //ipc::answer_with_name(msg_wrapper.msg(), "CTRL", ());
                            ipc::send_empty(service_name, "recv", "STOP");
                            println!("{} Done!", tid);
                            return;
                        },
                        _ => log::info!("{} Received {}", tid, a),
                    }
                },
                Err(err) => match err {
                    IpcError::Full => {
                        log::warn!("[{}] Full", tid);
                        thread::sleep(2000);
                    },
                    _ => log::error!("[{}] Error: {:?}", tid, err),
                },
            }
            thread::sleep(500);
        }
    } else if command == "mathserver" || command == "ms" {

        let service_name = "mathserver";

        let endpoint_ctrl = ipc::register_ipc_endpoint(service_name, "ctrl").unwrap();

        let math_fns: [(&str, fn(f64, f64) -> f64); 4] = [
            ("add", |i1, i2| i1 + i2),
            ("sub", |i1, i2| i1 - i2),
            ("mult", |i1, i2| i1 * i2),
            ("div", |i1, i2| i1 / i2),
        ];

        for (endpoint_name, _) in math_fns {
            ipc::register_ipc_endpoint(service_name, endpoint_name).unwrap();
        }

        let num_child_processes = 2;

        for _ in 0..num_child_processes {
            thread::start_application("ipctest", vec!["mathclient"]);
        }

        //thread::sleep(5000);

        let mut num_stop_req = 0;

        for (endpoint_name, math_fn) in math_fns {
            thread::create(move || {
                let tid = thread::current().unwrap().id();
                let debug_name = format!("[Mathserver - {}] {}", tid, endpoint_name);
                loop {
                    match ipc::receive_busy_polling(service_name, endpoint_name, 5000, 200) {
                        Ok(msg_wrapper) => {
                            let msg = msg_wrapper.msg();
                            log::info!("{} received {}", debug_name, msg);
                            let mut result: f64;
                            match msg.data() {
                                IpcMessageData::UsizeSlice { data, size } => {
                                    let numbers = &data[..*size];
                                    result = numbers[0] as f64;
                                    for n in numbers.iter().skip(1) {
                                        let f = *n as f64;
                                        result = (math_fn)(result, f);
                                    }
                                },
                                IpcMessageData::IsizeSlice { data, size } => {
                                    let numbers = &data[..*size];
                                    result = numbers[0] as f64;
                                    for n in numbers.iter().skip(1) {
                                        let f = *n as f64;
                                        result = (math_fn)(result, f);
                                    }
                                },
                                IpcMessageData::I32Slice { data, size } => {
                                    let numbers = &data[..*size];
                                    result = numbers[0] as f64;
                                    for n in numbers.iter().skip(1) {
                                        let f = *n as f64;
                                        result = (math_fn)(result, f);
                                    }
                                },
                                IpcMessageData::F64Slice { data, size } => {
                                    let numbers = &data[..*size];
                                    result = numbers[0];
                                    for n in numbers.iter().skip(1) {
                                        result = (math_fn)(result, *n);
                                    }
                                },
                                _ => {
                                    ipc::answer(msg, "ERROR", "Invalid Message Type!");
                                    continue;
                                },
                            }
                            log::info!("{} answer is {}", debug_name, result);
                            ipc::answer(msg, "answer", result);
                        },
                        Err(err) => {
                            if err != IpcError::Empty && err != IpcError::Timeout {
                                log::error!("[{}] Error: {:?}", tid, err);
                            }
                        },
                    }
                }
            });
        }

        loop {
            match endpoint_ctrl.receive_ref_blocking(5000) {
                Ok(msg_wrapper) => {
                    let msg = msg_wrapper.msg();
                    if msg.name() == "STOP" {
                        num_stop_req += 1;
                        log::info!("{} received STOP request from {} -- [{}/{}]", tid, msg.sender_tid(), num_stop_req, num_child_processes);
                        if num_stop_req >= num_child_processes {
                            log::info!("{} received [{}/{}] STOP requests, exiting...", tid, num_stop_req, num_child_processes);
                            return;
                        }
                    }
                },
                Err(err) => {
                    if err != IpcError::Empty {
                        log::error!("[{}] Error: {:?}", tid, err);
                    }
                },
            }
            thread::sleep(300);
        }
    } else if command == "mathclient" || command == "mc" {

        let math_fns = [
            "add",
            "sub",
            "mult",
            "div",
        ];
        let math_symb = [
            "+", "-", "*", "/",
        ];

        //let r = syscall(syscall::SystemCall::GetSystemTime, &[]).unwrap();
        //let limit = (r * 41) % 13;
        let limit = 2;

        let mut count = 0;

        loop {
            let r1 = syscall(syscall::SystemCall::GetSystemTime, &[]).unwrap();
            let r2 = syscall(syscall::SystemCall::GetSystemTime, &[]).unwrap();
            let f = syscall(syscall::SystemCall::GetSystemTime, &[]).unwrap();

            let ridx = (f * 7) % 4;
            let endpoint_name = math_fns[ridx];

            let n1 = (r1 * 1009) % 999;
            let n2 = (r2 * 1093) % 999;

            match ipc::send_and_receive_busy_polling("mathserver", endpoint_name, "MATH", &[n1, n2], 5000) {
                Ok(msg_wrapper) => {
                    let res = msg_wrapper.msg().data_as_f64_unchecked();
                    println!("[{}] Sent {} {} {}, received {}", tid, n1, math_symb[ridx], n2, res);
                    count += 1;
                },
                Err(err) => {
                    log::error!("[{}] Error: {:?}", tid, err);
                },
            }

            if count >= limit {
                ipc::send_empty("mathserver", "ctrl", "STOP");
                println!("[{}] Done", tid);
                return;
            }
        }
    } else {
        println!("Invalid argument {}", command)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MyStruct {
    a: usize,
    b: i16,
    c: ConstString<32>,
}

impl Display for MyStruct {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "( a: {}, b: {}, c: {} )", self.a, self.b, self.c)
    }
}

/// send message from string args, starting with the service_name at index 0 \
/// `args` needs to be at least 1 element
fn send_msg_from_args(args: &[String]) -> Result<usize, IpcError> {

    let service_name = args.get(0).unwrap();
    let endpoint_name = args.get(1).unwrap();
    let msg_name = args.get(2).cloned().unwrap_or("default".to_string());
    let msg_data_type = args.get(3).map(|s| s.as_str().into()).unwrap_or(IpcMessageType::Empty);

    match msg_data_type {
        IpcMessageType::Usize | IpcMessageType::UsizeSlice => {
            let content: Vec<usize> = args[4..].iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            ipc::send(service_name, endpoint_name, &msg_name, content.as_slice())
        },
        IpcMessageType::Isize | IpcMessageType::IsizeSlice => {
            let content: Vec<isize> = args[4..].iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            ipc::send(service_name, endpoint_name, &msg_name, content.as_slice())
        },
        IpcMessageType::I32 | IpcMessageType::I32Slice => {
            let content: Vec<i32> = args[4..].iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            ipc::send(service_name, endpoint_name, &msg_name, content.as_slice())
        },
        IpcMessageType::F64 | IpcMessageType::F64Slice => {
            let content: Vec<f64> = args[4..].iter().map(|arg| arg.parse().unwrap_or_default()).collect();
            ipc::send(service_name, endpoint_name, &msg_name, content.as_slice())
        },
        IpcMessageType::Empty => ipc::send_empty(service_name, endpoint_name, &msg_name),
        IpcMessageType::Raw => {
            ipc::send(service_name, endpoint_name, &msg_name, args.get(4).map(|s| s.as_bytes()).unwrap_or(&[]))
        },
        IpcMessageType::String => {
            ipc::send(service_name, endpoint_name, &msg_name, args.get(4).unwrap_or(&String::new()))
        },
        IpcMessageType::Struct => {
            unimplemented!("how would structs work with rust? (missing runtime reflection)");
        },
        _ => ipc::send_empty(service_name, endpoint_name, &msg_name),
    }
}