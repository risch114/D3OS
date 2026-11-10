#![no_std]
#![feature(generic_atomic)] // used by lfqueue
#![feature(integer_casts)]
#![allow(clippy::new_without_default)]

pub mod ipc_message;
pub mod ipc_message_numbers;
pub mod ipc_error;
pub mod ipc_mailbox;
pub mod queues;

extern crate alloc;

use alloc::string::ToString;
use syscall::{SystemCall, syscall};

use concurrent::{process, thread};

use crate::ipc_mailbox::IpcEndpoint;
use crate::ipc_message::{IpcMessage, IpcMessageContent, IpcMessageData, IpcMessageType, IpcMessageWrapper};
use crate::ipc_error::IpcError;

// Schnittstelle für Interprozesskommunikation

const USER_SPACE_MAILBOX_START: usize = 0x10100000000;

pub const SERVICE_NAME_LEN: usize = 64;
pub const SERVICE_NUM: usize = 4;

pub const ENDPOINT_NAME_LEN: usize = 64;
pub const ENDPOINT_NUM: usize = 8;

const MESSAGE_NAME_LEN: usize = 32;
//pub const MESSAGE_DATA_SIZE: usize = 0x100 - (size_of::<Uuid>() + size_of::<usize>() + MESSAGE_TAG_LEN);
pub const MESSAGE_DATA_SIZE: usize = 0x100;
const MAX_NUM_MESSAGES: usize = 16;

/// registers an endpoint with the given name \
/// returns an error if the endpoint already exists
// TODO what happens when we register multiple endpoints / multiple services for this endpoint
pub fn register_ipc_endpoint(service_name: &str, endpoint_name: &str) -> Result<&'static IpcEndpoint, IpcError> {

    let endpoint = ipc_mailbox::register_endpoint(service_name, endpoint_name)?;
    IpcError::convert_syscall_result(
        syscall(SystemCall::IpcRegister, &[core::ptr::from_ref(&service_name) as usize, core::ptr::from_ref(&endpoint_name) as usize, core::ptr::from_ref(endpoint) as usize])
    ).map(|_| endpoint)
}

pub fn find_ipc_endpoint(service_name: &str, endpoint_name: &str) -> Result<&'static IpcEndpoint, IpcError> {
    ipc_mailbox::find_endpoint(service_name, endpoint_name)
}

pub fn remove_ipc_endpoint(svc_name: &str) -> Result<usize, IpcError> {
    let svc_name_ptr = core::ptr::from_ref(&svc_name);
    IpcError::convert_syscall_result(syscall(SystemCall::IpcDelete, &[svc_name_ptr as usize]))
}

pub fn create_msg<T: IpcMessageContent>(content: T) -> IpcMessageData {
    content.to_data()
}

/// send message content directly
/// 
/// this copies the given data, to create a new IpcMessageData struct
pub fn send<T: IpcMessageContent>(service: &str, endpoint: &str, msg_name: &str, content: T) -> Result<usize, IpcError> {
    send_internal(service, endpoint, msg_name, content.msg_type(), content.to_bytes(), ("callback", &process::current().id().to_string()))
}
pub fn send_with_callback<T: IpcMessageContent>(service: &str, endpoint: &str, msg_name: &str, callback: (&str, &str), content: T) -> Result<usize, IpcError> {
    send_internal(service, endpoint, msg_name, content.msg_type(), content.to_bytes(), callback)
}

pub fn send_empty(service: &str, endpoint: &str, msg_name: &str) -> Result<usize, IpcError> {
    send_internal(service, endpoint, msg_name, IpcMessageType::Empty, &[], ("callback", &process::current().id().to_string()))
}

/// send a custom data structure over ipc
pub fn send_struct<T: Copy>(service: &str, endpoint: &str, msg_name: &str, message: &T) -> Result<usize, IpcError> {
    let bytes = unsafe { any_as_bytes(message) };
    send_internal(service, endpoint, msg_name, IpcMessageType::Struct, bytes, ("callback", &process::current().id().to_string()))
}

/// send message and block waiting for answer
pub fn send_and_receive<T: IpcMessageContent>(service: &str, endpoint: &str, msg_name: &str, content: T, timeout_ms: usize) -> Result<impl IpcMessageWrapper, IpcError> {
    let pid_string = process::current().id().to_string();
    let callback = ("callback", pid_string.as_str());
    match send_internal(service, endpoint, msg_name, content.msg_type(), content.to_bytes(), callback) {
        Ok(_) => {
            let mut counter = 0;
            loop {
                let rec = receive_blocking(callback.0, callback.1, timeout_ms);
                match rec {
                    // TODO find specific answer message
                    Ok(_) => return rec,
                    Err(err) => {
                        match err {
                            IpcError::Empty => {
                                thread::sleep(200);
                                counter += 200;
                                if counter >= timeout_ms {
                                    return Err(IpcError::Timeout);
                                }
                            },
                            _ => return Err(err),
                        }
                    },
                }
            }
        },
        Err(err) => Err(err),
    }
}

/// send message and wait for answer
pub fn send_and_receive_busy_polling<T: IpcMessageContent>(service: &str, endpoint: &str, msg_name: &str, content: T, timeout_ms: usize) -> Result<impl IpcMessageWrapper, IpcError> {
    let pid_string = process::current().id().to_string();
    let callback = ("callback", pid_string.as_str());
    match send_internal(service, endpoint, msg_name, content.msg_type(), content.to_bytes(), callback) {
        Ok(_) => {
            let mut counter = 0;
            loop {
                let rec = receive_busy_polling(callback.0, callback.1, timeout_ms, 100);
                match rec {
                    // TODO find specific answer message
                    Ok(_) => return rec,
                    Err(err) => {
                        match err {
                            IpcError::Empty => {
                                thread::sleep(200);
                                counter += 200;
                                if counter >= timeout_ms {
                                    return Err(IpcError::Timeout);
                                }
                            },
                            _ => return Err(err),
                        }
                    },
                }
            }
        },
        Err(err) => Err(err),
    }
}

pub fn answer<T: IpcMessageContent>(original_message: &IpcMessage, answer_name: &str, content: T) -> Result<usize, IpcError> {
    send_internal(
        original_message.callback.service_name.as_str_unchecked(),
        original_message.callback.endpoint_name.as_str_unchecked(),
        answer_name,
        content.msg_type(), 
        content.to_bytes(), 
        ("callback", &process::current().id().to_string()),
    )
}

pub fn answer_with_callback<T: IpcMessageContent>(original_message: &IpcMessage, answer_name: &str, callback: (&str, &str), content: T) -> Result<usize, IpcError> {
    send_internal(
        original_message.callback.service_name.as_str_unchecked(),
        original_message.callback.endpoint_name.as_str_unchecked(),
        answer_name,
        content.msg_type(), 
        content.to_bytes(), 
        callback
    )
}

pub struct IpcSendSyscallParams<'a> {
    pub target_service_name: &'a str,
    pub target_endpoint_name: &'a str,
    pub msg_name: &'a str,
    pub msg_type: IpcMessageType,
    pub data: &'a [u8],
    pub callback: (&'a str, &'a str),
}

pub fn send_internal(service_name: &str, target_endpoint: &str, msg_name: &str, msg_type: IpcMessageType, data: &[u8], callback: (&str, &str)) -> Result<usize, IpcError> {

    // register callback endpoint
    let res = register_ipc_endpoint(callback.0, callback.1);

    // check for relevant error | if the error is that the endpoint already exists, continue
    if let Err(err) = res.as_ref() {
        match err {
            IpcError::EndpointAlreadyExists => {},
            _ => return Err(*err),
        }
    }

    let params = IpcSendSyscallParams {
        target_service_name: service_name,
        target_endpoint_name: target_endpoint,
        msg_name,
        msg_type,
        data,
        callback
    };

    IpcError::convert_syscall_result(syscall(SystemCall::IpcSend, &&params))
}

pub fn receive(service_name: &str, endpoint_name: &str) -> Result<impl IpcMessageWrapper + use<>, IpcError> {
    match ipc_mailbox::find_endpoint(service_name, endpoint_name) {
        Ok(endpoint) => endpoint.receive_ref(),
        Err(err) => Err(err),
    }
}

pub fn receive_answer() -> Result<impl IpcMessageWrapper + use<>, IpcError> {
    receive("", &process::current().id().to_string())
}

pub struct IpcRcvBlockingSyscallParams {
    pub endpoint_name_ptr: *const u8,
    pub service_name_len: usize,
    pub timeout_ms: usize,
}

/// block and wait for a message \
/// will stop waiting after timeout ms
pub fn receive_blocking(service_name: &str, endpoint_name: &str, timeout_ms: usize) -> Result<impl IpcMessageWrapper + use<>, IpcError> {
    match ipc_mailbox::find_endpoint(service_name, endpoint_name) {
        Ok(endpoint) => endpoint.receive_ref_blocking(timeout_ms),
        Err(err) => Err(err),
    }
}

pub fn receive_busy_polling(service_name: &str, endpoint_name: &str, timeout_ms: usize, interval_ms: usize) -> Result<impl IpcMessageWrapper + use<>, IpcError> {
    let time_now = syscall::syscall(syscall::SystemCall::GetSystemTime, &[]).unwrap();
    let time_timeout = time_now + timeout_ms;
    loop {
        let rec = receive(service_name, endpoint_name);
        if rec.is_ok() {
            return rec;
        }
        if let Err(err) = rec {
            match err {
                IpcError::Empty => {} // keep spinning if empty
                _ => return Err(err), // abort if there is a different error
            }
        }
        if syscall::syscall(syscall::SystemCall::GetSystemTime, &[]).unwrap() >= time_timeout {
            return Err(IpcError::Timeout);
        }
        if interval_ms > 0 {
            thread::sleep(interval_ms);
        }
    }
}

/// Reinterprets an arbitrary struct T into bytes.
///
/// # Safety
///
/// Uses `core::slice::from_raw_parts internally`. Check its docs for the safety requirements.
pub unsafe fn any_as_bytes<T>(any: &T) -> &[u8] {
    unsafe { core::slice::from_raw_parts((any as *const T) as *const u8, size_of::<T>()) }
}

/// Reinterprets a byte slice into an arbitrary struct T.
///
/// # Safety
///
/// Manually dereferences the byte slice into the struct T. Will most likely only work correctly when used on a byte slice created by `any_as_bytes`.
pub unsafe fn bytes_as_any<T>(bytes: &[u8]) -> &T {
    unsafe { &*(bytes.as_ptr() as *const T) }
}