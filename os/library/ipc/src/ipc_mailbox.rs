use core::sync::atomic::AtomicBool;

use alloc::vec::Vec;
use alloc::string::{String, ToString};
use spin::Mutex;

use crate::USER_SPACE_MAILBOX_START;
use crate::{ENDPOINT_NUM, SERVICE_NUM, ipc_endpoint::IpcEndpoint};


const MAILBOX_INIT: AtomicBool = AtomicBool::new(false);

pub struct IpcMailbox {
    services: [IpcService; SERVICE_NUM],
    slots: Mutex<[Option<IpcMailboxSlot>; SERVICE_NUM]>, // this is only ever locked when creating or removing services (i.e. rarely)
}

struct IpcMailboxSlot {
    name: String,
    index: usize,
    ptr: *const IpcService,
}

impl IpcMailbox {

    pub fn new() -> Self {
        Self { services: core::array::from_fn(|_| IpcService::new()), slots: Mutex::new(core::array::from_fn(|_| None)) }
    }

    pub fn find(&self, name: &str) -> Option<&'static IpcService> {
        let idx = self.index(name)?;
        let ptr = self.slots.lock()[idx].as_ref()?.ptr;
        unsafe { Some(&*ptr) }
    }

    pub fn index(&self, name: &str) -> Option<usize> {
        match self.slots.lock().iter()
            .filter(|slot| slot.is_some())
            .map(|opt| opt.as_ref().unwrap())
            .find(|slot| slot.name == name) {
                Some(slot) => Some(slot.index),
                None => None,
            }
    }

    pub fn register(&self, name: &str) -> Option<&'static IpcService> {

        let l = self.slots.lock().len();

        if l == 0 {
            return unsafe { self.register_idx(name, 0) };
        }

        // service already exists, return it
        match self.find(name) {
            Some(r) => return Some(r),
            None => {},
        }

        // maximum number of services reached
        if l >= SERVICE_NUM {
            return None;
        }
        
        match self.find_free_idx() {
            Some(idx) => unsafe { self.register_idx(name, idx) },
            None => None,
        }
    }

    pub fn delete(&self, name: &str) -> bool {

        let l = self.slots.lock().len();

        if l == 0 {
            return false;
        }
        
        let found_idx = self.index(name);

        match found_idx {
            Some(idx) => {
                self.slots.lock()[idx] = None;
                true
            },
            None => false,
        }
    }

    fn find_free_idx(&self) -> Option<usize> {
        for (idx, slot) in self.slots.lock().iter().enumerate() {
            if slot.is_none() {
                return Some(idx);
            }
        }
        None
    }

    unsafe fn register_idx(&self, name: &str, idx: usize) -> Option<&'static IpcService> {
        let svc_ptr = unsafe { self.services.as_ptr().add(idx) };
        let slot = IpcMailboxSlot {
            name: name.to_string(),
            index: idx,
            ptr: svc_ptr,
        };
        self.slots.lock()[idx] = Some(slot);
        unsafe { Some(&*svc_ptr) }
    }
}

pub struct IpcService {
    endpoints: [IpcEndpoint; ENDPOINT_NUM],
    free_slots: Mutex<Vec<usize>>,
}

impl IpcService {
    pub fn take_slot(&self) -> Option<usize> {
        self.free_slots.lock().pop()
    }
    pub fn free_slot(&self, slot: usize) {
        self.free_slots.lock().push(slot);
    }
}

pub fn init_mailbox() -> &'static IpcMailbox {
    if !MAILBOX_INIT.load(core::sync::atomic::Ordering::Relaxed) {
        unsafe {
            (USER_SPACE_MAILBOX_START as *mut IpcMailbox).write(IpcMailbox::new())
        };
        MAILBOX_INIT.store(true, core::sync::atomic::Ordering::Relaxed);
    }
    unsafe { &*(USER_SPACE_MAILBOX_START as *mut IpcMailbox) }
}