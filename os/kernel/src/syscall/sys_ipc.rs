use core::str;

use alloc::vec::Vec;
use alloc::{format, slice};
use d3os::{any_as_bytes, bytes_as_any, copy_from_slice_sizechecked};
use ipc::ipc_endpoint::IpcEndpoint;
use ipc::{IpcRcvBlockingSyscallParams, IpcSendSyscallParams, MESSAGE_DATA_SIZE};
use ipc::queues::IpcQueue;
use ipc::ipc_error::IpcError;
use log::error;
use naming::shared_types::OpenOptions;
use syscall::return_vals::{self, Errno, SyscallResult};
use uuid::Uuid;

use crate::naming::api as nameservice;
use crate::naming::servicefs::Endpoint;
use crate::{process::core_local_storage::scheduler, sync::wait_queue::WaitQueue, timer};

static WAIT_QUEUE: WaitQueue = WaitQueue::new();

pub extern "sysv64" fn sys_ipc_register(service_name_ptr: *const &str, endpoint_ptr: *const IpcEndpoint) -> isize {

    let service_name = unsafe { *service_name_ptr };
    let endpoint = unsafe { &*endpoint_ptr };

    return_vals::convert_syscall_result_to_ret_code(register_new_endpoint(service_name, endpoint))
}

fn register_new_endpoint(service_name: &str, endpoint: &IpcEndpoint) -> Result<usize, Errno> {

    let endpoint_name = endpoint.name.lock().clone();

    // try creating new service directory
    let svc_filehandle = nameservice::open(&format!("/svc/{service_name}"), OpenOptions::DIRECTORY);
    if svc_filehandle.is_ok() {
        nameservice::close(svc_filehandle.unwrap());
    } else {
        nameservice::mkdir(&format!("/svc/{service_name}"))?;
    }

    let endpoint_fh = nameservice::open(&format!("/svc/{service_name}/{endpoint_name}"), OpenOptions::CREATE)?;

    let endpoint_ptr = core::ptr::from_ref(endpoint);
    let bytes = unsafe { any_as_bytes!(&Endpoint::new(endpoint_ptr as usize, scheduler().current_ids().0)) };
    nameservice::write(endpoint_fh, bytes);
    nameservice::close(endpoint_fh);

    Ok(0)
}

pub extern "sysv64" fn sys_ipc_delete(service_name_ptr: *const &str, endpoint_name_ptr: *const &str) -> isize {
    
    let service_name = unsafe { *service_name_ptr };
    let endpoint_name = unsafe { *endpoint_name_ptr };

    let res = if endpoint_name == "" {
        remove_service(service_name)
    } else {
        remove_endpoint(service_name, endpoint_name)
    };

    match res {
        Ok(_) => return_vals::convert_syscall_result_to_ret_code(SyscallResult::Ok(0)),
        Err(err) => return_vals::convert_syscall_result_to_ret_code(SyscallResult::Err(err)),
    }
}

fn remove_endpoint(service_name: &str, endpoint_name: &str) -> Result<usize, Errno> {
    let svc_dir_fh = nameservice::open(&format!("/svc/{service_name}"), OpenOptions::READWRITE)?;
    let svc_dir_list = nameservice::list_dir(svc_dir_fh)?;
    
    let fh = nameservice::open(&format!("/svc/{service_name}/{endpoint_name}"), OpenOptions::READWRITE)?;
    nameservice::delete(fh);

    // close entire service, if this is the last endpoint
    if svc_dir_list.len() == 1 {
        nameservice::delete(svc_dir_fh);
    } else {
        nameservice::close(svc_dir_fh);
    }

    Ok(0)
}

fn remove_service(service_name: &str) -> Result<usize, Errno> {
    let svc_dir_fh = nameservice::open(&format!("/svc/{service_name}"), OpenOptions::READWRITE)?;

    for (_, name) in &nameservice::list_dir(svc_dir_fh)? {
        let fh = nameservice::open(&format!("/svc/{service_name}/{name}"), OpenOptions::READWRITE)?;
        nameservice::delete(fh);
    }

    nameservice::delete(svc_dir_fh);
    Ok(0)
}

pub fn remove_endpoints_for_process(process_id: &Uuid) -> Result<usize, Errno> {
    let svc_root_fh = nameservice::open("/svc", OpenOptions::DIRECTORY)?;
    for (_, service_name) in &nameservice::list_dir(svc_root_fh)? {
        let svc_dir_fh = nameservice::open(&format!("/svc/{service_name}"), OpenOptions::DIRECTORY & OpenOptions::READWRITE)?;
        let all_endpoints = nameservice::list_dir(svc_dir_fh)?;
        let mut num_removed = 0;
        for (_, endpoint_name) in &all_endpoints {
            let endpoint_fh = nameservice::open(&format!("/svc/{service_name}/{endpoint_name}"), OpenOptions::READWRITE)?;
            let mut buf = [0; size_of::<Endpoint>()];
            nameservice::read(endpoint_fh, &mut buf);
            let endpoint = unsafe { d3os::bytes_as_any!(buf; Endpoint) };
            if endpoint.process_id() == process_id {
                nameservice::delete(endpoint_fh);
                num_removed += 1;
            } else {
                nameservice::close(endpoint_fh);
            }
        }
        if num_removed == all_endpoints.len() {
            nameservice::delete(svc_dir_fh);
        } else {
            nameservice::close(svc_dir_fh);
        }
    }
    nameservice::close(svc_root_fh);
    Ok(0)
}

pub extern "sysv64" fn sys_ipc_send(params_ptr: *const IpcSendSyscallParams) -> isize {

    let params = unsafe { &*params_ptr };

    let bytes = params.data;

    if bytes.len() > MESSAGE_DATA_SIZE {
        error!("ipc message is to large, aborting...");
        return IpcError::MsgTooLarge.into();
    }
    
    let service_name = params.target_service_name;
    let endpoint_name = params.target_endpoint_name;
    
    if let Ok(fh) = nameservice::open(&format!("/svc/{service_name}/{endpoint_name}"), OpenOptions::READONLY) {

        let mut bytes = [0; size_of::<Endpoint>()];
        let read_success = nameservice::read(fh, &mut bytes);

        if read_success.is_err() { return IpcError::Generic.into(); }

        let endpoint = unsafe { bytes_as_any!(&bytes; Endpoint) };
        nameservice::close(fh);

        let ipc_queue = endpoint.queue();

        let name = params.msg_name;
        let msg_type = params.msg_type;
        let callback = params.callback;

        let res = ipc_queue.send(name, &bytes, msg_type, &scheduler().current_thread().process().id(), scheduler().current_thread().id(), callback);

        match res {
            Ok(ok) => {
                ipc_queue.unblock_waiting(|p, t| scheduler().unsleep(p, t));
                return return_vals::convert_syscall_result_to_ret_code(SyscallResult::Ok(ok));
            },
            Err(err) => return err.into(),
        }

    }

    IpcError::Generic.into()
}

pub extern "sysv64" fn sys_ipc_receive_blocking(params_ptr: *const IpcRcvBlockingSyscallParams) -> isize {

    unimplemented!("use blocking receive in ipc library");

    /*

    let params = unsafe { &*params_ptr };
    
    let endpoint_name = unsafe { core::str::from_utf8_unchecked(core::slice::from_raw_parts(params.endpoint_name_ptr, params.service_name_len)) };
    
    if let Ok(fh) = nameservice::open(&format!("/svc/{}", endpoint_name), OpenOptions::READONLY) {

        // read ServiceObject using u8 buffer
        let mut buf = [0_u8; size_of::<ServiceObject>()];
        let _res = nameservice::read(fh, buf.as_mut_slice());
        nameservice::close(fh);
        let service = unsafe { &(*(buf.as_ptr() as *const ServiceObject)).service };

        let endpoints = unsafe { (service.ipc_endpoints_addr as *const IpcEndpoints).as_ref_unchecked() };
        if let Some(ipc_queue) = endpoints.get(endpoint_name) {
            
            let current_time = timer().systime_ms();
            let timeout_time = current_time + params.timeout_ms;

            WAIT_QUEUE.wait(|| ipc_queue.messages_left() > 0 || timer().systime_ms() >= timeout_time, "wating for ipc receive");

            let num_messages = ipc_queue.messages_left();
            if num_messages > 0 {
                return return_vals::convert_syscall_result_to_ret_code(SyscallResult::Ok(num_messages));
            } else {
                return IpcError::Timeout.into();
            }
        }
    }
    IpcError::Generic.into()
    */
}