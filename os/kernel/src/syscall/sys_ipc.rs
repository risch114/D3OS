use core::str;

use alloc::{format, slice};
use d3os::copy_from_slice_sizechecked;
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

    // try create a directory in /svc/<name> to register the service
    let fh = match nameservice::open(&format!("/svc/{}", service_name), OpenOptions::CREATE & OpenOptions::DIRECTORY) {
        Ok(fh) => {
            fh
        }
        Err(err) => {
            if err == Errno::EEXIST {
                // service already exists, open
                match nameservice::open(&format!("/svc/{}", service_name), OpenOptions::READONLY & OpenOptions::DIRECTORY) {
                    Ok(fh) => {
                        fh
                    },
                    Err(err) => {
                        return return_vals::convert_syscall_result_to_ret_code(SyscallResult::Err(err));
                    },
                }
            } else {
                return return_vals::convert_syscall_result_to_ret_code(SyscallResult::Err(err));
            }
        }
    };

    nameservice::close(fh);

    // TODO: check if endpoint already exists

    match nameservice::open(&format!("/svc/{}/{}", service_name, endpoint.name.lock().as_str_unchecked()), OpenOptions::CREATE & OpenOptions::DIRECTORY) {
        Ok(fh) => {
            let endpoint_addr_bytes = usize::to_le_bytes(endpoint_ptr as usize);
            nameservice::write(fh, &endpoint_addr_bytes);
            nameservice::close(fh);
        },
        Err(err) => {
            return return_vals::convert_syscall_result_to_ret_code(SyscallResult::Err(err));
        },
    }

    // now we register the endpoint
    nameservice::close(fh);
    return_vals::convert_syscall_result_to_ret_code(SyscallResult::Ok(0))
}

pub extern "sysv64" fn sys_ipc_delete(name_ptr: *const u8, name_len: usize) -> isize {
    
    let bytes = unsafe { slice::from_raw_parts(name_ptr, name_len) };
    let name = str::from_utf8(bytes).unwrap();

    // remove the file in /svc/<name> to delete this process' ipc endpoint
    match nameservice::open(&format!("/svc/{}", name), OpenOptions::READWRITE) {
        Ok(fh) => {
            nameservice::delete(fh);
            return_vals::convert_syscall_result_to_ret_code(SyscallResult::Ok(0))
        }
        Err(err) => return_vals::convert_syscall_result_to_ret_code(SyscallResult::Err(err))
    }
}

pub fn remove_endpoints_for_process(process_id: &Uuid) {
    if let Ok(svc_dir_fh) = nameservice::open("/svc", OpenOptions::DIRECTORY) {
        // get all services
        if let Ok(list) = nameservice::list_dir(svc_dir_fh) {
            for (svc_dir, svc_name) in list {
                if let Ok(fh) = nameservice::open(&format!("/svc/{}", svc_name), OpenOptions::READWRITE) {
                    let mut buf = [0_u8; core::mem::size_of::<Endpoint>()];
                    nameservice::read(fh, &mut buf);
                    let service = unsafe { &(*(buf.as_ptr() as *const ServiceObject)).service };
                    if service.owning_pid == *process_id {
                        nameservice::delete(fh);
                    }
                }
            }
        }
    }
}

pub extern "sysv64" fn sys_ipc_send(params_ptr: *const IpcSendSyscallParams) -> isize {

    let params = unsafe { &*params_ptr };

    let bytes = unsafe { core::slice::from_raw_parts(params.data.0, params.data.1) };

    if bytes.len() > MESSAGE_DATA_SIZE {
        error!("ipc message is to large, aborting...");
        return IpcError::MsgTooLarge.into();
    }
    
    let service_name = unsafe { core::str::from_utf8_unchecked(core::slice::from_raw_parts(params.target_endpoint_name.0, params.target_endpoint_name.1)) };
    
    if let Ok(fh) = nameservice::open(&format!("/svc/{}", service_name), OpenOptions::READONLY) {

        // read ServiceObject using u8 buffer
        let mut buf = [0_u8; core::mem::size_of::<ServiceObject>()];
        let read_success = nameservice::read(fh, buf.as_mut_slice());
        nameservice::close(fh);

        if read_success.is_err() { return IpcError::Generic.into(); }

        let service = unsafe { &(*(buf.as_ptr() as *const ServiceObject)).service };

        let endpoints = unsafe { (service.ipc_endpoints_addr as *const IpcEndpoints).as_ref_unchecked() };
        if let Some(ipc_queue) = endpoints.get(service_name) {
            let name = unsafe { core::str::from_utf8_unchecked(core::slice::from_raw_parts(params.msg_name.0, params.msg_name.1)) };
            let msg_type = params.msg_type;
            let callback = unsafe { core::str::from_utf8_unchecked(core::slice::from_raw_parts(params.callback.0, params.callback.1)) };

            let res = ipc_queue.send(name, bytes, msg_type, &scheduler().current_thread().process().id(), scheduler().current_thread().id(), callback);

            match res {
                Ok(ok) => {
                    ipc_queue.unblock_waiting(|p, t| scheduler().unsleep(p, t));
                    return return_vals::convert_syscall_result_to_ret_code(SyscallResult::Ok(ok));
                },
                Err(err) => return err.into(),
            }
        }
    }

    IpcError::Generic.into()
}

pub extern "sysv64" fn sys_ipc_receive_blocking(params_ptr: *const IpcRcvBlockingSyscallParams) -> isize {

    let params = unsafe { &*params_ptr };
    
    let endpoint_name = unsafe { core::str::from_utf8_unchecked(core::slice::from_raw_parts(params.endpoint_name_ptr, params.service_name_len)) };
    
    if let Ok(fh) = nameservice::open(&format!("/svc/{}", endpoint_name), OpenOptions::READONLY) {

        // read ServiceObject using u8 buffer
        let mut buf = [0_u8; core::mem::size_of::<ServiceObject>()];
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
}