//
//
// ##### Parts of scheduler used solely for multicore #####
//
//
use crate::process::thread::{Thread, ThreadState};
use crate::process::scheduler;
use crate::process::scheduler::{ReadyState, Scheduler};
use crate::process::core_local_storage::{cls, current_core_id, scheduler};
use crate::device::apic::get_apic_id;
use crate::apic;
use crate::per_cpu_ref;

use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use alloc::sync::Arc;

use log::{debug, info};
use thingbuf::mpsc::Sender;
use uuid::Uuid;

impl Scheduler {

    /// Checks whether the current core should balance its threads.
    /// returns own_threads > (nbr_threads /nbr_cpus +1)
    pub(super) fn should_balance_now(&self) -> bool {
        let total_threads = scheduler::active_thread_count(true) as u32;
        let own_threads = read_rq_len();
        let nbr_cpus = scheduler::cpu_count();
        if own_threads > (total_threads / nbr_cpus + 1) {
            /*debug!("Scheduler{}: total_threads: {}, own_threads: {}, cpus: {}",
                current_core_id(), nbr_threads, own_threads, nbr_cpus);*/
            return true
        }
        false
    }

    /// Balances the threads on the current core by moving one thread from the tail to the target core.
    /// Target core is the core with the least number of threads.
    /// Returns the new state of the scheduler. (needed for mutable access)
    pub(super) fn balance_once(&self, state: &mut ReadyState) {
        let own_load = read_rq_len() as usize;
        if own_load <= 1 {
            //debug!("Scheduler: Cannot balance, current load ({:?}) is too low!", own_load);
            return;
        }

        if let Some((target_core, target_load)) = self.find_less_loaded_core() {
            if own_load >= target_load + 2 {
                let amount = ((own_load-target_load)/4)+1;
                for _ in 0..amount {
                    // Move one thread from the tail to the target
                    let thread_opt = state.ready_queue.pop_front();
                    if let Some(thread) = thread_opt {
                        // If we can't migrate this thread, skip it.
                        // This makes us migrate one less thread than we wanted,
                        // but this should be okay in general.
                        if !thread.can_migrate() {
                            info!("cannot migrate {thread:?}, skipping");
                            state.ready_queue.push_back(thread);
                            continue;
                        }

                        // Check if the migrating thread is the last thread on this core that has used the FPU.
                        // If so, we need to reset `last_fpu_thread` to None.
                        // We do not need to store the FPU context of the migrating thread,
                        // as we always take a thread from the ready queue and never the current thread.
                        if state.last_fpu_thread.is_some() {
                            if state.last_fpu_thread.as_ref().unwrap().id() == thread.id() {
                                state.last_fpu_thread = None;
                            }
                        }
                        let _tid = thread.id();
                        let w = MessageItem::new_thread(thread);
                        dec_rq_len();
                        if let Ok(_r) = schedule_on(target_core, w) {
                            debug!(" Scheduler{}: Scheduled thread {} on core {}", current_core_id(), _tid, target_core);
                        }
                    }
                }
            }
        }
    }

    /// Finds the core with the least number of threads.
    /// returns (target_core, target_load)
    fn find_less_loaded_core(&self) -> Option<(usize, usize)> {
        // Inspect per-core exported metrics
        let mut curr: usize = 0;
        let mut min = read_rq_len_remote(0);
        for i in 1..scheduler::cpu_count() as usize {
            if min > read_rq_len_remote(i) {
                min = read_rq_len_remote(i);
                curr = i;
            }
        }
        let own = read_rq_len();
        if min >= own { return None; }
        Some((curr, min as usize))
    }

    /// Finds the core with the most number of threads.
    /// returns the target's Core Id
    fn find_more_loaded_core(&self) -> Option<usize> {
        // Inspect per-core exported metrics
        let mut curr: usize = 0;
        let mut max = read_rq_len_remote(0);
        for i in 1..scheduler::cpu_count() as usize {
            if max < read_rq_len_remote(i) {
                max = read_rq_len_remote(i);
                curr = i;
            };
        }
        let own = read_rq_len();
        if max <= own || max < 2 { return None; }
        Some(curr)
    }

    /// Forces a core with more than 2 threads to migrate one through a Reschedule IPI.
    /// (Sends a reschedule IPI that will result in a migration within switch_thread())
    pub fn look_for_overloaded_core(&self) {
        let overloaded_core = self.find_more_loaded_core();
        match overloaded_core {
            None => return,
            Some(target_id) => send_reschedule_ipi(target_id)
        }
    }

    /// Handle a command received via inbox, using the already-held ready_state lock (`state`).
    fn handle_inbox_cmd(&self, cmd: MessageCmd, state: &mut ReadyState) {
        match cmd {
            // Wake local join-waiters and add to readyQueue
            MessageCmd::JoinTargetExited { tid } => {
                let mut join_map = self.join_map.lock();

                if let Some(join_list) = join_map.get_mut(&tid) {
                    for waiter in join_list.drain(..) {
                        waiter.set_state(ThreadState::Running);
                        state.ready_queue.push_front(waiter);
                        inc_rq_len();
                    }
                    join_map.remove(&tid);
                }
            }
            // If the thread is locally blocked, requeue it.
            MessageCmd::Deblock { pid, tid } => {
                let mut blocked_list = self.blocked_list.lock();
                if scheduler::is_thread_alive(tid) == false { return; }

                if let Some(pos) = blocked_list
                    .iter().position(|t| t.id() == tid && t.process().id() == pid)
                {
                    let thread = blocked_list.remove(pos);
                    thread.set_state(ThreadState::Running);
                    state.ready_queue.push_front(thread);
                    inc_rq_len();
                }
            }
            // if you have this thread, kill it
            MessageCmd::Kill { tid } => {
                if scheduler::is_thread_alive(tid) == false { return; }
                let thread = self.current_thread();
                if thread.id() == tid { //cant kill itself, reschedule for other thread
                    let _ = schedule_on(current_core_id() as usize, MessageItem::Cmd(MessageCmd::Kill { tid }));
                    let _ = schedule_on_all_others(MessageItem::Cmd(MessageCmd::Kill { tid }));    //if target migrates until then
                    return;
                }
                self.kill_locally(tid, state);
            }
        }
    }
}


//// Multicore support  ////

/// Helper struct to store shared public information of a core
///     rq_len: approximate runqueue length (owner updates, others read)
///     resched_flag: indicates whether another core requested a reschedule
///     tx: sender for other cores to send messages to this one
///     apic_id: stored at initialization, used to translate cpu_id to apic_id for IPI's
#[repr(align(64))]
pub struct PerCpuRef {
    pub(super) rq_len: AtomicU32,
    resched_flag: AtomicBool,
    tx: Sender<Option<MessageItem>>,    // producers (remote cores)
    apic_id: AtomicU32,
}
unsafe impl Sync for PerCpuRef {}
impl PerCpuRef { pub fn new(tx: Sender<Option<MessageItem>>) -> Self {
    Self { rq_len: AtomicU32::new(0), resched_flag: AtomicBool::new(false),
        tx, apic_id: AtomicU32::new(0) } } }


/// Wrapper enum to store either a runnable thread or a small cross-core command.
/// (Should be kept "small" since it lives in per-core inboxes and is drained in scheduler paths)
#[derive(Clone)]
pub enum MessageItem {
    Thread(Arc<Thread>),
    Cmd(MessageCmd),
}

impl MessageItem {
    #[inline]
    pub fn new_thread(thread: Arc<Thread>) -> Self {
        MessageItem::Thread(thread)
    }
    #[inline]
    pub fn new_cmd(cmd: MessageCmd) -> Self {
        MessageItem::Cmd(cmd)
    }
}


/// Commands that a core can request another core to perform locally
#[derive(Clone)]
pub enum MessageCmd {
    /// "Thread with `tid` exited somewhere; if you have join-waiters for it locally, wake them."
    JoinTargetExited { tid: usize },

    /// "If you have this thread in your local blocked list, wake it."
    Deblock { pid: Uuid, tid: usize },

    /// "If you have this thread, kill it."
    Kill { tid: usize },
}

/// Called only once by each owner core during startup to set the PER_CPU_SCHED apic_id
pub fn set_inbox_apic_id(id: usize) {
    let curr_id = id;
    let apic_id = get_apic_id();
    info!("Setting inbox{} apic_id to {}", curr_id, apic_id);
    per_cpu_ref(curr_id).apic_id.store(apic_id, Ordering::Release);
}

/// Returns the apic_id of the core with the given id
pub fn per_cpu_apic_id(cpu_id: usize) -> u32 {
    per_cpu_ref(cpu_id).apic_id.load(Ordering::Acquire)
}

/// Returns a reference to the PerCpuSched struct of the current core
#[inline]
pub fn per_cpu_ref_curr() -> &'static PerCpuRef {
    per_cpu_ref(current_core_id() as usize)
}

/// Returns a reference to the sender out of the PerCpuSched struct of the core with the given id
#[inline]
pub fn per_cpu_sender(id: usize) -> &'static Sender<Option<MessageItem>> {
    &per_cpu_ref(id).tx
}

/// Schedules a thread (wrapped in a MessageItem) on a remote core with the target_id
/// Sends a reschedule IPI to wake that core up, if it was idle.
pub fn schedule_on(target_id: usize, item: MessageItem) -> Result<(), MessageItem> {
    let pc = per_cpu_ref(target_id);
    match pc.tx.try_send(Some(item)) {
        Ok(()) => {
            send_reschedule_ipi(target_id);
            Ok(())
        }
        Err(e) => Err(e.into_inner().unwrap()), // unwrap: we sent Some(_)
    }
}

/// Schedules a cmd (wrapped in a MessageItem) on ALL remote cores
/// Does not send reschedule IPI's since only one core needs to actually do something
pub fn schedule_on_all_others(item: MessageItem) {
    let own_id = current_core_id() as usize;
    for i in 0..scheduler::cpu_count() as usize {
        if i != own_id {
            let pc = per_cpu_ref(i);
            pc.tx.try_send(Some(item.clone())).expect("Failed to send message to remote core!");
        }
    }
}

/// drains the inbox from the cls into the ready queue; 10 items max per call
/// automatically calls inc_rq_len()
pub fn drain_inbox_into_ready(max: usize, state: &mut ReadyState) {
    let mut drained_threads = 0usize;
    for _ in 0..max {
        match cls().try_recv() {
            Ok(Some(item)) => match item {
                MessageItem::Thread(thread) => {

                    state.ready_queue.push_front(thread);
                    inc_rq_len();
                    drained_threads += 1;
                }
                MessageItem::Cmd(cmd) => {
                    scheduler().handle_inbox_cmd(cmd, state);
                }
            },
            Ok(None) => {
                log::error!("cpu{}: inbox returned None (unexpected)", current_core_id());
                break;
            }
            Err(_) => break,
        }
    }
    if drained_threads > 0 {
        clear_resched_flag();
        debug!("cpu{}: drained {} thread(s) into ready_queue", current_core_id(), drained_threads);
    }
}

/// Sends a Reschedule IPI to wake the core with the given id up, if it was idle.
fn send_reschedule_ipi(target_id: usize) {
    apic().send_reschedule(per_cpu_apic_id(target_id))
}
/// Owner function to set the reschedule flag of the current core.
pub fn set_resched_flag() {
    per_cpu_ref(current_core_id() as usize).resched_flag.store(true, Ordering::Release);
}
/// Owner function to clear the reschedule flag of the current core.
pub fn clear_resched_flag() {
    per_cpu_ref(current_core_id() as usize).resched_flag.store(false, Ordering::Release);
}
/// Owner function to read the reschedule flag of the current core.
pub fn read_resched_flag() -> bool {
    per_cpu_ref(current_core_id() as usize).resched_flag.load(Ordering::Acquire)
}
/// Owner function to increase the runqueue length of the current core.
pub fn inc_rq_len() {
    let id = current_core_id() as usize;
    per_cpu_ref(id).rq_len.fetch_add(1, Ordering::Relaxed);
}
/// Owner function to decrease the runqueue length of the current core.
pub fn dec_rq_len() {
    let id = current_core_id() as usize;
    per_cpu_ref(id).rq_len.fetch_sub(1, Ordering::Release); // Release for stronger publication before donating
}
/// Owner function to read the runqueue length of the current core.
pub fn read_rq_len() -> u32 {
    per_cpu_ref(current_core_id() as usize).rq_len.load(Ordering::Acquire)
}
/// Remote Reader function to read the runqueue length of the given Core
pub fn read_rq_len_remote(target_id: usize) -> u32 {
    per_cpu_ref(target_id).rq_len.load(Ordering::Acquire)
}