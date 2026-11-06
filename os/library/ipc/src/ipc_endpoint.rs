use core::sync::atomic::AtomicBool;

use d3os::const_string::ConstString;
use spin::RwLock;
use syscall::syscall;
use syscall::SystemCall;

use crate::queues::IpcQueue;
use crate::queues::IpcQueueType;
use crate::{USER_SPACE_MAILBOX_START, ipc_error::IpcError};

const ENDPOINT_NAME_LEN: usize = 64;
const ENDPOINT_NUM: usize = 8;

static ENDPOINTS_INIT: AtomicBool = AtomicBool::new(false);

pub struct IpcEndpoints {
    endpoints: [RwLock<ConstString<ENDPOINT_NAME_LEN>>; ENDPOINT_NUM],
    queues: [IpcQueueType; ENDPOINT_NUM]
}

impl IpcEndpoints {
    pub fn new() -> Self {
        Self {
            endpoints: [const { RwLock::new(ConstString::new()) }; _],
            queues: core::array::from_fn(|_| IpcQueueType::new())
        }
    }
    pub fn get(&self, name: &str) -> Option<&IpcQueueType> {
        let pos = self.endpoints.iter().position(|endpoint_name| endpoint_name.read().as_str_unchecked() == name)?;
        Some(&self.queues[pos])
    }
    pub fn try_register(&self, svc_name: &str) -> Result<usize, IpcError> {

        let opt_duplicate = self.endpoints.iter().find(|name| name.read().as_str_unchecked() == svc_name);

        if opt_duplicate.is_some() {
            return Err(IpcError::EndpointAlreadyExists);
        }

        match self.endpoints.iter().position(|name| name.read().is_empty()) {
            Some(pos) => {
                self.endpoints[pos].write().set(svc_name);
                Ok(pos)
            },
            None => Err(IpcError::Full),
        }
    }
}

/// registers an endpoint with the given name \
/// returns an error if the endpoint already exists
pub fn register_ipc_endpoint(endpoint_name: &str) -> Result<usize, IpcError> {
    // TODO what happens when we register multiple endpoints / multiple services for this endpoint

    let endpoints_ptr = USER_SPACE_MAILBOX_START as *mut IpcEndpoints;
    // if queue is not initialized, create new queue
    if ENDPOINTS_INIT.compare_exchange(false, true, core::sync::atomic::Ordering::Relaxed, core::sync::atomic::Ordering::Relaxed).is_ok() {
        unsafe { endpoints_ptr.write(IpcEndpoints::new()) };
    }

    // try creating a new endpoint
    unsafe { (*endpoints_ptr).try_register(endpoint_name)?; }

    let name_ptr = endpoint_name.as_ptr();
    let name_len = endpoint_name.len();
    IpcError::convert_syscall_result(syscall(SystemCall::IpcRegister, &[name_ptr as usize, name_len]))
}

pub fn remove_ipc_endpoint(svc_name: &str) -> Result<usize, IpcError> {
    let name_ptr = svc_name.as_ptr();
    let name_len = svc_name.len();
    IpcError::convert_syscall_result(syscall(SystemCall::IpcDelete, &[name_ptr as usize, name_len]))
}