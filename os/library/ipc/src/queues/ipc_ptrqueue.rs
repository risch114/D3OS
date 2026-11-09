use alloc::sync::Arc;
use spin::Mutex;
use uuid::Uuid;

use crate::MAX_NUM_MESSAGES;
use crate::queues::atomic_ringbuffer::Ringbuffer;
use crate::queues::IpcError;
use crate::ipc_message::{IpcMessage, IpcMessageType} ;

const S: usize = MAX_NUM_MESSAGES;

pub struct IpcPtrQueue {
    data: [IpcMessage; S],
    free_slots: Ringbuffer<usize, S>,
    used_slots: Ringbuffer<usize, S>,
    waiting_threads: Mutex<[Option<(uuid::Uuid, usize)>; S]> // used for blocking waits
}

impl IpcPtrQueue {
    pub fn new() -> IpcPtrQueue  {
        let s = Self {
            data: core::array::from_fn(|_| IpcMessage::default()),
            free_slots: Ringbuffer::new(),
            used_slots: Ringbuffer::new(),
            waiting_threads: Mutex::new([const { None }; _])
        };
        // init free slots
        for i in 0..S { s.free_slots.push(i); }
        s
    }
}

impl IpcPtrQueue {

    fn msg_ptr(&self, slot: usize) -> *mut IpcMessage {
        assert!(slot < S, "slot index out of queue bounds");
        let mut_ptr = self.data.as_ptr() as *mut IpcMessage;
        unsafe {
            mut_ptr.add(slot)
        }
    }
    
    pub fn send(&self, msg: &IpcMessage) -> Result<usize, IpcError> {

        // check if there is a free slot available
        if let Some(free_slot) = self.free_slots.pop() {
            
            self.used_slots.push(free_slot); // mark slot as used

            // we are the sole 'owner' of the slot
            // this means we can modify the data 'safely'
            unsafe {
                (*self.msg_ptr(free_slot)).copy_from(msg);
            }
            Ok(0)
        } else {
            Err(IpcError::Full)
        }
    }
    
    pub fn send_from_parts(&self, msg_name: &str, data_bytes: &[u8], data_type: IpcMessageType, sender_pid: &Uuid, sender_tid: usize, callback: (&str, &str)) -> Result<usize, IpcError> {

        // check if there is a free slot available
        if let Some(free_slot) = self.free_slots.pop() {

            self.used_slots.push(free_slot); // mark slot as used

            // we are the sole 'owner' of the slot
            // this means we can modify the data 'safely'
            unsafe {
                (*self.msg_ptr(free_slot)).set_from_data_bytes(msg_name, data_bytes, data_type, *sender_pid, sender_tid, callback);
            }
            Ok(0)
        } else {
            Err(IpcError::Full)
        }
    }

    pub fn receive(self: &Arc<Self>) -> Result<PtrQueueMsgWrapper, IpcError> {

        // check if there is a used slot (the queue contains data)
        if let Some(used_slot) = self.used_slots.pop() {
            
            // return a ptr to the data in the used slot, using the Wrapper, to enable automatic free on drop
            return Ok(PtrQueueMsgWrapper { msg: self.msg_ptr(used_slot), queue: Arc::clone(self), slot: used_slot });
        }

        Err(IpcError::Empty)
    }

    pub fn receive_blocking(self: &Arc<Self>, timeout_ms: usize) -> Result<PtrQueueMsgWrapper, IpcError> {

        if self.num_used() > 0 {
            return self.receive();
        }

        let timeout_time = syscall::syscall(syscall::SystemCall::GetSystemTime, &[]).unwrap() + timeout_ms;
        let free = self.waiting_threads.lock().iter().position(|opt| opt.is_none());
        if let Some(idx) = free {
            let pid = concurrent::process::current().id();
            let tid = concurrent::thread::current().unwrap().id();
            loop {
                if self.num_used() > 0 {
                    return self.receive();
                }
                if syscall::syscall(syscall::SystemCall::GetSystemTime, &[]).unwrap() >= timeout_time {
                    return Err(IpcError::Timeout);
                }

                let mut guard = self.waiting_threads.lock();
                
                guard[idx] = Some((pid, tid));
                drop(guard);
                concurrent::thread::sleep(timeout_ms);
            }
        }

        Err(IpcError::Empty)
    }
    
    pub fn unblock_waiting<F: Fn(uuid::Uuid, usize) -> bool>(&self, unblock_fn: F) {
        let mut guard = self.waiting_threads.lock();
        let num_waiting = guard.iter().filter(|opt| opt.is_some()).count();
        if num_waiting > 0 {
            for opt in guard.iter_mut() {
                if let Some((pid, tid)) = opt {
                    let unblock_success = unblock_fn(*pid, *tid);
                    if unblock_success {
                        *opt = None;
                    }
                    // TODO when unblock fails here: thread was registered as waiting but missed unblock
                }
            }
        }
    }

    /// frees the value at the given slot
    fn free(&self, slot: usize) {
        // busy wait here; this should never happen, there should never be more slots then this ringbuffers size
        while !self.free_slots.push(slot) {
            concurrent::thread::switch();
        }
    }
    
    pub fn num_free(&self) -> usize {
        self.free_slots.num_elements()
    }
    
    pub fn num_used(&self) -> usize {
        self.used_slots.num_elements()
    }
}

/// struct used for re-freeing slot once msg is Dropped
pub struct PtrQueueMsgWrapper {
    msg: *const IpcMessage,
    queue: *const IpcPtrQueue,
    slot: usize,
}
impl PtrQueueMsgWrapper {
    pub fn msg(&self) -> &IpcMessage {
        // we are the only one holding a ptr to the message -> this should be safe
        unsafe { &*self.msg }
    }
}

impl Drop for PtrQueueMsgWrapper {
    fn drop(&mut self) {
        self.queue.free(self.slot);
    }
}