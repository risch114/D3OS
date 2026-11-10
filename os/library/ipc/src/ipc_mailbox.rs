use core::sync::atomic::AtomicBool;

use alloc::string::String;
use alloc::sync::Arc;
use d3os::slot_map::SlotMap;
use spin::Mutex;

use crate::USER_SPACE_MAILBOX_START;
use crate::{ENDPOINT_NUM, SERVICE_NUM};
use crate::ipc_message::{IpcMessage, IpcMessageWrapper};
use crate::ipc_error::IpcError;
use crate::queues::IpcQueueType;


const MAILBOX_INIT: AtomicBool = AtomicBool::new(false);

struct StaticMailboxes<'a> {
    default_mailbox: IpcService<'a>,
    mailboxes: IpcMailbox<'a>,
}

impl StaticMailboxes<'static> {
    pub fn new() -> Self {
        Self { default_mailbox: IpcService::new(), mailboxes: IpcMailbox::new() }
    }
}

pub struct IpcMailbox<'a> {
    slot_map: SlotMap<'a, IpcService<'a>, SERVICE_NUM>, // this is only ever locked when creating or removing services (i.e. rarely)
}

impl<'a> IpcMailbox<'a> {

    pub fn new() -> Self {
        Self { slot_map: SlotMap::new() }
    }

    pub fn find_service(&self, service_name: &str) -> Option<&'a IpcService> {
        self.slot_map.find(service_name)
    }

    pub fn register_service(&self, service_name: &str) -> Option<&'a IpcService> {
        self.slot_map.occupy_slot(service_name)
    }

    pub unsafe fn delete_service(&self, service_name: &str) -> bool {
        unsafe { self.slot_map.clear_slot(service_name) }
    }
}

pub struct IpcService<'a> {
    slot_map: SlotMap<'a, IpcEndpoint, ENDPOINT_NUM>,
}

impl<'a> IpcService<'a> {

    pub fn new() -> Self {
        Self { slot_map: SlotMap::new() }
    }

    pub fn find_endpoint(&self, endpoint_name: &str) -> Option<&'a IpcEndpoint> {
        self.slot_map.find(endpoint_name)
    }

    pub fn register_endpoint(&self, endpoint_name: &str) -> Option<&'a IpcEndpoint> {
        self.slot_map.occupy_slot(endpoint_name)
    }

    pub unsafe fn delete_endpoint(&self, endpoint_name: &str) -> bool {
        unsafe { self.slot_map.clear_slot(endpoint_name) }
    }
}

impl<'a> Default for IpcService<'a> {
    fn default() -> Self {
        Self::new()
    }
}

pub static ENDPOINTS_INIT: AtomicBool = AtomicBool::new(false);

pub struct IpcEndpoint {
    pub service_name: Mutex<Arc<String>>,
    pub endpoint_name: Mutex<Arc<String>>,
    pub queue: IpcQueueType,
}

impl IpcEndpoint {

    pub fn new() -> Self {
        Self { service_name: Mutex::new(Arc::new(String::new())), endpoint_name: Mutex::new(Arc::new(String::new())), queue: IpcQueueType::new() }
    }

    pub fn receive_ref(&self) -> Result<IpcMessageRef, IpcError> {
        let (slot, message_ptr) = self.queue.receive()?;
        Ok(IpcMessageRef {
            service_name: Arc::clone(&self.service_name.lock()),
            endpoint_name: Arc::clone(&self.endpoint_name.lock()),
            message_ptr,
            slot
        })
    }

    pub fn receive_copy(&self) -> Result<IpcMessage, IpcError> {
        let (slot, message_ptr) = self.queue.receive()?;
        let message = unsafe {
            let tmp = message_ptr.read();
            self.free(slot);
            tmp
        };
        Ok(message)
    }

    pub fn receive_ref_blocking(&self, timeout_ms: usize) -> Result<IpcMessageRef, IpcError> {
        let (slot, message_ptr) = self.queue.receive_blocking(timeout_ms)?;
        Ok(IpcMessageRef {
            service_name: Arc::clone(&self.service_name.lock()),
            endpoint_name: Arc::clone(&self.endpoint_name.lock()),
            message_ptr,
            slot
        })
    }

    pub fn receive_copy_blocking(&self, timeout_ms: usize) -> Result<IpcMessage, IpcError> {
        let (slot, message_ptr) = self.queue.receive_blocking(timeout_ms)?;
        let message = unsafe {
            let tmp = message_ptr.read();
            self.free(slot);
            tmp
        };
        Ok(message)
    }

    unsafe fn free(&self, slot: usize) {
        unsafe { self.queue.free_slot_unchecked(slot) };
    }
}

impl Default for IpcEndpoint {
    fn default() -> Self {
        Self::new()
    }
}



pub fn init_mailboxes() -> &'static StaticMailboxes<'static> {
    let ptr = USER_SPACE_MAILBOX_START as *mut StaticMailboxes;
    if !MAILBOX_INIT.load(core::sync::atomic::Ordering::Relaxed) {
        unsafe {
            ptr.write(StaticMailboxes::new())
        };
        MAILBOX_INIT.store(true, core::sync::atomic::Ordering::Relaxed);
    }
    unsafe { &*ptr }
}

pub fn register_service(service_name: &str) -> Option<&'static IpcService<'static>> {
    let mailboxes = init_mailboxes();
    if service_name == "" {
        Some(&mailboxes.default_mailbox)
    } else {
        mailboxes.mailboxes.register_service(service_name)
    }
}

pub fn register_endpoint(service_name: &str, endpoint_name: &str) -> Result<&'static IpcEndpoint, IpcError> {
    match register_service(service_name) {
        Some(service) => {
            match service.find_endpoint(endpoint_name) {
                None => {
                    match service.register_endpoint(endpoint_name) {
                        Some(endpoint) => Ok(endpoint),
                        None => Err(IpcError::Full),
                    }
                },
                Some(_) => Err(IpcError::EndpointAlreadyExists),
            }
        },
        None => Err(IpcError::Full),
    }
}

pub fn find_endpoint(service_name: &str, endpoint_name: &str) -> Result<&'static IpcEndpoint, IpcError> {
    let mailboxes = init_mailboxes();
    let find_service = if service_name == "" {
        Some(&mailboxes.default_mailbox)
    } else {
        mailboxes.mailboxes.find_service(service_name)
    };
    match find_service {
        Some(service) => {
            match service.find_endpoint(endpoint_name) {
                Some(endpoint) => Ok(endpoint),
                None => Err(IpcError::EndpointNotFound),
            }
        },
        None => Err(IpcError::ServiceNotFound),
    }
}

pub struct IpcMessageRef {
    service_name: Arc<String>,
    endpoint_name: Arc<String>,
    message_ptr: *mut IpcMessage,
    slot: usize,
}

impl IpcMessageRef {
    pub fn msg_checked(&self) -> Result<&'static IpcMessage, IpcError> {
        match find_endpoint(&self.service_name, &self.endpoint_name) {
            Ok(endpoint) => {
                unsafe {
                    let ptr = endpoint.queue.slot_unchecked(self.slot);
                    Ok(&*ptr)
                }
            },
            Err(_) => Err(IpcError::Invalid),
        }
    }
    pub unsafe fn msg_unchecked(&self) -> &'static IpcMessage {
        unsafe {
            &*self.message_ptr
        }
    }
}

impl IpcMessageWrapper for IpcMessageRef {
    fn msg(&self) -> &IpcMessage {
        unsafe { self.msg_unchecked() }
    }
}

impl Drop for IpcMessageRef {
    fn drop(&mut self) {
        match find_endpoint(&self.service_name, &self.endpoint_name) {
            Ok(endpoint) => {
                unsafe { endpoint.free(self.slot) };
            },
            Err(_) => {},
        }
    }
}