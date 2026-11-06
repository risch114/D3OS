use core::sync::atomic::AtomicBool;

use alloc::vec::Vec;
use d3os::const_string::ConstString;
use spin::Mutex;

use crate::queues::IpcQueue;
use crate::queues::IpcQueueType;
use crate::ipc_error::IpcError;

pub const ENDPOINT_NAME_LEN: usize = 64;
pub const ENDPOINT_NUM: usize = 8;

pub static ENDPOINTS_INIT: AtomicBool = AtomicBool::new(false);

pub struct IpcEndpoint {
    pub name: Mutex<ConstString<ENDPOINT_NAME_LEN>>,
    pub queue: IpcQueueType,
}

impl IpcEndpoint {
    pub fn new() -> Self {
        Self { name: Mutex::new(ConstString::new()), queue: IpcQueueType::new() }
    }
}

pub struct IpcEndpoints {
    endpoints: [IpcEndpoint; ENDPOINT_NUM],
    free_slots: Mutex<Vec<usize>>,
}

impl IpcEndpoints {
    pub fn new() -> Self {
        let mut free_slots = Vec::with_capacity(ENDPOINT_NUM);
        for i in (0..ENDPOINT_NUM).rev() {
            free_slots.push(i);
        }
        Self {
            endpoints: core::array::from_fn(|_| IpcEndpoint::new()),
            free_slots: Mutex::new(free_slots),
        }
    }

    pub fn find(&self, name: &str) -> Option<&IpcEndpoint> {
        let pos = self.endpoints.iter().position(|e| e.name.lock().as_str_unchecked() == name)?;
        Some(&self.endpoints[pos])
    }

    pub fn try_register(&self, name: &str) -> Result<&IpcEndpoint, IpcError> {

        if self.find(name).is_some() {
            return Err(IpcError::EndpointAlreadyExists);
        }

        match self.free_slots.lock().pop() {
            Some(free_slot) => {
                self.endpoints[free_slot].name.lock().set(name);
                Ok(&self.endpoints[free_slot])
            },
            None => Err(IpcError::Full),
        }
    }

    pub fn try_remove(&self, name: &str) -> Result<usize, IpcError> {

        let find_pos = self.endpoints.iter().position(|e| e.name.lock().as_str_unchecked() == name);

        let Some(pos) = find_pos else {
            return Err(IpcError::EndpointNotFound);
        };

        self.endpoints[pos].name.lock().clear();
        self.endpoints[pos].queue.clear();

        self.free_slots.lock().push(pos);

        Ok(pos)
    }
}