use alloc::{string::{String, ToString}, vec::Vec};
use concurrent::process::Process;
use spin::{Mutex, Once};

static SERVICE_MANAGER: Once<Mutex<ServiceManager>> = Once::new();

pub fn service_manager() -> &'static Mutex<ServiceManager> {
    SERVICE_MANAGER.call_once(|| Mutex::new(ServiceManager::new()))
}

pub struct ServiceManager {
    services: Vec<Service>,
}

pub struct Service {
    pub name: String,
    pub process: Process,
}

impl ServiceManager {

    pub fn new() -> Self {
        Self { services: Vec::new() }
    }

    pub fn start(&mut self, name: &str) -> Result<Process, &str> {

        if self.find(name).is_some() {
            return Err("Service already exists");
        }

        if let Some(thread) = concurrent::thread::start_application(name, Vec::new()) {
            let process = thread.process();
            self.services.push(Service { name: name.to_string(), process });
            Ok(process)
        } else {
            Err("Could not start application")
        }
    }

    pub fn stop(&mut self, name: &str) -> Result<(), &str> {

        match self.find(name) {
            Some((svc, idx)) => {
                let res = svc.process.kill();
                if res.is_ok() {
                    self.services.remove(idx);
                    Ok(())
                } else {
                    Err("Could not kill process")
                }
            },
            None => Err("Service does not exist"),
        }
    }

    pub fn find(&self, name: &str) -> Option<(&Service, usize)> {
        let pos = self.services.iter().position(|svc| svc.name == name);
        pos.map(|p| (self.services.get(p).unwrap(), p))
    }

    pub fn list(&self) -> &Vec<Service> {
        &self.services
    }
}