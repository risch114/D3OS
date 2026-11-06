use core::sync::atomic::AtomicUsize;

use alloc::{string::{String, ToString}, sync::Arc, vec::Vec};
use d3os::{const_string::ConstString, copy_from_slice_sizechecked};
use ipc::{ipc_endpoint::IpcEndpoint, queues::IpcQueueType};
use naming::shared_types::DirEntry;
use spin::rwlock::RwLock;
use syscall::return_vals::Errno;
use uuid::Uuid;

use crate::{consts, naming::{stat::Mode, traits::{DirectoryObject, FileObject, FileSystem, NamedObject}}, process::core_local_storage::scheduler};

#[derive(Debug)]
pub struct Service {
    pub name: String,
    pub ipc_endpoints_addr: usize,
    pub owning_pid: Uuid,
}

pub struct ServiceFs {
    service_root_dir: Arc<ServiceRootDir>,
}

impl ServiceFs {
    pub fn new() -> ServiceFs {
        ServiceFs {
            service_root_dir: Arc::new(ServiceRootDir::new()),
        }
    }
}

impl FileSystem for ServiceFs {
    fn root_dir(&self) -> Arc<dyn DirectoryObject> {
        self.service_root_dir.clone()
    }
}

#[derive(Debug)]
pub struct ServiceRootDir {
    services: RwLock<Vec<Arc<ServiceDir>>>
}

impl ServiceRootDir {
    pub fn new() -> Self {
        Self { services: RwLock::new(Vec::new()) }
    }
}

impl DirectoryObject for ServiceRootDir {
    
    fn lookup(&self, name: &str) -> Result<super::traits::NamedObject, Errno> {
        // the current directory is /svc
        // check for /svc/<name>
        match self.services.read().iter().find(|&svc_dir| svc_dir.name.as_str() == name) {
            Some(svc_dir) => Ok(svc_dir.directory_obj()),
            None => Err(Errno::ENOENT)
        }
    }
    
    fn list(&self) -> Result<Vec<(NamedObject, String)>, Errno> {
        let mut list = Vec::new();
        for svc_dir in self.services.read().iter() {
            list.push((svc_dir.directory_obj(), String::clone(&svc_dir.name)));
        }
        Ok(list)
    }

    fn readdir(&self, index: usize) -> Result<Option<DirEntry>, Errno> {
        match self.services.read().get(index) {
            Some(svc_dir) => Ok(Some(DirEntry { file_type: naming::shared_types::FileType::Directory, name: String::clone(&svc_dir.name) })),
            None => Ok(None),
        }
    }

    fn create_file(&self, _name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        Err(Errno::ENOTSUP)
    }

    fn create_dir(&self, name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        let service_dir = Arc::new(ServiceDir::new(name));
        self.services.write().push(Arc::clone(&service_dir));
        Ok(NamedObject::DirectoryObject(service_dir))
    }

    fn create_pipe(&self, _name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        Err(Errno::ENOTSUP)
    }

    fn stat(&self) -> Result<super::stat::Stat, Errno> {
        Err(Errno::ENOTSUP)
    }
    
    fn delete_file(&self, _name: &str) -> Result<(), Errno> {
        Err(Errno::ENOTSUP)
    }
    
    fn delete_dir(&self, name: &str) -> Result<(), Errno> {
        let mut services_lock = self.services.write();
        // check if service with name exists
        if let Some(pos) = services_lock.iter().position(|svc| svc.name.as_str() == name) {
            // remove service
            services_lock.swap_remove(pos);
            Ok(())
        } else {
            // Error if service does not exist
            Err(Errno::EEXIST)
        }
    }
    
    fn delete_pipe(&self, _name: &str) -> Result<(), Errno> {
        Err(Errno::ENOTSUP)
    }
}

#[derive(Debug)]
pub struct ServiceDir {
    name: Arc<String>,
    endpoints: Arc<RwLock<Vec<Endpoint>>>,
}

impl ServiceDir {

    pub fn new(name: &str) -> Self {
        Self { name: Arc::new(name.to_string()), endpoints: Arc::new(RwLock::new(Vec::new())) }
    }

    pub fn directory_obj(&self) -> super::traits::NamedObject {
        NamedObject::DirectoryObject(Arc::new(ServiceDir { name: Arc::clone(&self.name), endpoints: Arc::clone(&self.endpoints) }))
    }
}

impl DirectoryObject for ServiceDir {
    
    fn lookup(&self, name: &str) -> Result<super::traits::NamedObject, Errno> {
        // the current directory is /svc/service_name/
        // check for /svc/service_name/<name>
        match self.endpoints.read().iter().find(|&endpoint| endpoint.name() == name) {
            Some(endpoint) => Ok(NamedObject::FileObject(Arc::new(Endpoint::copy(&endpoint)))),
            None => Err(Errno::ENOENT)
        }
    }
    
    fn list(&self) -> Result<Vec<(NamedObject, String)>, Errno> {
        let mut list = Vec::new();
        for endpoint in self.endpoints.read().iter() {
            list.push((NamedObject::FileObject(Arc::new(Endpoint::copy(&endpoint))), endpoint.name().to_string()));
        }
        Ok(list)
    }

    fn readdir(&self, index: usize) -> Result<Option<DirEntry>, Errno> {
        match self.endpoints.read().get(index) {
            Some(endpoint) => Ok(Some(DirEntry { file_type: naming::shared_types::FileType::Regular, name: endpoint.name().to_string() })),
            None => Ok(None),
        }
    }

    fn create_file(&self, _name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        let endpoint = Endpoint { address: AtomicUsize::new(0) };
        self.endpoints.write().push(Endpoint::copy(&endpoint));
        Ok(NamedObject::FileObject(Arc::new(endpoint)))
    }

    fn create_dir(&self, _name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        Err(Errno::ENOTSUP)
    }

    fn create_pipe(&self, _name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        Err(Errno::ENOTSUP)
    }

    fn stat(&self) -> Result<super::stat::Stat, Errno> {
        Err(Errno::ENOTSUP)
    }
    
    fn delete_file(&self, name: &str) -> Result<(), Errno> {
        let mut endpoints_lock = self.endpoints.write();
        // check if service with name exists
        if let Some(pos) = endpoints_lock.iter().position(|endpoint| endpoint.name() == name) {
            // remove service
            endpoints_lock.swap_remove(pos);
            Ok(())
        } else {
            // Error if service does not exist
            Err(Errno::EEXIST)
        }
    }
    
    fn delete_dir(&self, _name: &str) -> Result<(), Errno> {
        Err(Errno::ENOTSUP)
    }
    
    fn delete_pipe(&self, _name: &str) -> Result<(), Errno> {
        Err(Errno::ENOTSUP)
    }
}

#[derive(Debug)]
pub struct Endpoint {
    address: AtomicUsize,
}

impl Endpoint {

    pub fn copy(other: &Self) -> Self {
        Self { address: AtomicUsize::new(other.address()) }
    }

    pub fn address(&self) -> usize {
        self.address.load(core::sync::atomic::Ordering::Relaxed)
    }

    pub fn endpoint(&self) -> &IpcEndpoint {
        unsafe { &*(self.address() as *const IpcEndpoint) }
    }

    pub fn name(&self) -> ConstString<{ ipc::ipc_endpoint::ENDPOINT_NAME_LEN }> {
        self.endpoint().name.lock().clone()
    }

    pub fn queue(&self) -> &IpcQueueType {
        &self.endpoint().queue
    }
}

impl FileObject for Endpoint {
    
    /// read `Endpoint` into `buf`
    fn read(&self, buf: &mut [u8], _offset: usize, _options: naming::shared_types::OpenOptions) -> Result<usize, syscall::return_vals::Errno> {
        let selfslice = unsafe { core::slice::from_raw_parts((self as *const Self) as *const u8, core::mem::size_of::<Self>()) };
        d3os::copy_from_slice_sizechecked!(buf, selfslice);
        Ok(0)
    }

    fn write(&self, buf: &[u8], _offset: usize, _options: naming::shared_types::OpenOptions) -> Result<usize, Errno> {

        let mut le_bytes = [0; size_of::<usize>()];
        copy_from_slice_sizechecked!(le_bytes, buf);
        let endpoint_addr_virt = usize::from_le_bytes(le_bytes);

        let process = scheduler().current_thread().process();
        let endpoint_addr_phys = process
            .virtual_address_space
            .translate_addr(endpoint_addr_virt)
            .ok_or(Errno::EACCES)?;
            //.expect("ipc endpoint isnt mapped into virt addr space");
        
        self.address.store(endpoint_addr_phys, core::sync::atomic::Ordering::Relaxed);
        Ok(endpoint_addr_phys)
    }

    fn stat(&self) -> Result<super::stat::Stat, Errno> {
        Err(Errno::ENOTSUP)
    }
}