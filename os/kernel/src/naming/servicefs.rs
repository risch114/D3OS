use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use alloc::{string::{String, ToString}, sync::Arc, vec::Vec};
use d3os::{any_as_bytes, bytes_as_any, const_string::ConstString, copy_from_slice_sizechecked};
use ipc::{ipc_endpoint::IpcEndpoint, queues::IpcQueueType};
use naming::shared_types::DirEntry;
use spin::rwlock::RwLock;
use syscall::return_vals::Errno;
use uuid::Uuid;

use crate::process::core_local_storage::scheduler;
use crate::naming::stat::Mode;
use crate::naming::traits::{DirectoryObject, FileObject, FileSystem, NamedObject};


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
        let s = Self { services: RwLock::new(Vec::new()) };
        s.services.write().push(Arc::new(ServiceDir::new("callback")));
        s
    }
}

impl DirectoryObject for ServiceRootDir {
    
    fn lookup(&self, name: &str) -> Result<super::traits::NamedObject, Errno> {
        // the current directory is /svc
        // check for /svc/<name>
        match self.services.read().iter().find(|&svc_dir| svc_dir.name.as_str() == name) {
            Some(svc_dir) => {
                let svc_dir_clone = Arc::clone(svc_dir);
                Ok(NamedObject::DirectoryObject(svc_dir_clone))
            },
            None => Err(Errno::ENOENT)
        }
    }
    
    fn list(&self) -> Result<Vec<(NamedObject, String)>, Errno> {
        let mut list = Vec::new();
        for svc_dir in self.services.read().iter() {
            let svc_dir_clone = Arc::clone(svc_dir);
            list.push((NamedObject::DirectoryObject(svc_dir_clone), String::clone(&svc_dir.name)));
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
    name: String,
    endpoints: RwLock<Vec<Arc<EndpointFile>>>,
}

impl ServiceDir {

    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            endpoints: RwLock::new(Vec::new())
        }
    }
}

impl DirectoryObject for ServiceDir {
    
    fn lookup(&self, name: &str) -> Result<super::traits::NamedObject, Errno> {
        // the current directory is /svc/service_name/
        // check for /svc/service_name/<name>
        match self.endpoints.read().iter().find(|&endpoint| endpoint.name() == name) {
            Some(endpoint) => {
                let endpoint_clone = Arc::clone(&endpoint);
                Ok(NamedObject::FileObject(endpoint_clone))
            },
            None => Err(Errno::ENOENT)
        }
    }
    
    fn list(&self) -> Result<Vec<(NamedObject, String)>, Errno> {
        let mut list = Vec::new();
        for endpoint in self.endpoints.read().iter() {
            let endpoint_clone = Arc::clone(&endpoint);
            list.push((NamedObject::FileObject(endpoint_clone), endpoint.name().to_string()));
        }
        Ok(list)
    }

    fn readdir(&self, index: usize) -> Result<Option<DirEntry>, Errno> {
        match self.endpoints.read().get(index) {
            Some(endpoint) => Ok(Some(DirEntry { file_type: naming::shared_types::FileType::Regular, name: endpoint.name().to_string() })),
            None => Ok(None),
        }
    }

    fn create_file(&self, name: &str, _mode: Mode) -> Result<NamedObject, Errno> {
        let endpoint = Arc::new(EndpointFile::new(name.to_string(), 0, Uuid::nil()));
        self.endpoints.write().push(Arc::clone(&endpoint));
        Ok(NamedObject::FileObject(endpoint))
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

#[derive(Debug, Clone, Copy)]
pub struct Endpoint {
    /// physical endpoint address
    address: usize,
    /// owning process id
    process_id: Uuid,
}

impl Endpoint {

    pub fn new(address: usize, pid: Uuid) -> Self {
        Self { address, process_id: pid }
    }

    pub fn process_id(&self) -> &Uuid {
        &self.process_id
    }

    pub fn endpoint(&self) -> &IpcEndpoint {
        unsafe { &*(self.address as *const IpcEndpoint) }
    }

    pub fn name(&self) -> ConstString<{ ipc::ipc_endpoint::ENDPOINT_NAME_LEN }> {
        self.endpoint().name.lock().clone()
    }

    pub fn queue(&self) -> &IpcQueueType {
        &self.endpoint().queue
    }
}

#[derive(Debug)]
struct EndpointFile {
    name: String,
    /// physical endpoint address
    address: AtomicUsize,
    /// owning process id
    process_id: (AtomicU64, AtomicU64),
}

impl EndpointFile {

    pub fn new(name: String, address: usize, pid: Uuid) -> Self {
        let pair = pid.as_u64_pair();
        Self { name, address: AtomicUsize::new(address), process_id: (AtomicU64::new(pair.0), AtomicU64::new(pair.1)) }
    }

    pub fn as_endpoint(&self) -> Endpoint {
        Endpoint::new(
            self.address.load(Ordering::Relaxed), 
            Uuid::from_u64_pair(self.process_id.0.load(Ordering::Relaxed), self.process_id.1.load(Ordering::Relaxed))
        )
    }

    pub fn set(&self, address: usize, process_id: Uuid) {
        self.address.store(address, Ordering::Relaxed);
        let pair = process_id.as_u64_pair();
        self.process_id.0.store(pair.0, Ordering::Relaxed);
        self.process_id.1.store(pair.1, Ordering::Relaxed);
    }

    pub fn ipc_endpoint(&self) -> &IpcEndpoint {
        unsafe { &*(self.address.load(Ordering::Relaxed) as *const IpcEndpoint) }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn queue(&self) -> &IpcQueueType {
        &self.ipc_endpoint().queue
    }
}

impl FileObject for EndpointFile {
    
    /// read `Endpoint` into `buf`
    fn read(&self, buf: &mut [u8], _offset: usize, _options: naming::shared_types::OpenOptions) -> Result<usize, syscall::return_vals::Errno> {
        let endpoint = self.as_endpoint();
        let bytes = unsafe { any_as_bytes!(&endpoint) };
        d3os::copy_from_slice_sizechecked!(buf, bytes);
        Ok(0)
    }

    fn write(&self, buf: &[u8], _offset: usize, _options: naming::shared_types::OpenOptions) -> Result<usize, Errno> {

        let endpoint = unsafe { bytes_as_any!(buf; Endpoint) };

        let process = scheduler().current_thread().process();
        let endpoint_addr_phys = process
            .virtual_address_space
            .translate_addr(endpoint.address)
            .ok_or(Errno::EACCES)?;
            //.expect("ipc endpoint isnt mapped into virt addr space");
        
        self.set(endpoint_addr_phys, endpoint.process_id);
        Ok(size_of::<Endpoint>())
    }

    fn stat(&self) -> Result<super::stat::Stat, Errno> {
        Err(Errno::ENOTSUP)
    }
}