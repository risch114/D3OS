#![no_std]

mod service_manager;

extern crate alloc;

use alloc::{string::String, vec::Vec};
use runtime::env::args;
#[allow(unused_imports)]
use runtime::*;
use terminal::println;

use crate::service_manager::service_manager;

static LOGGER: logger::Logger = logger::Logger::new();

#[unsafe(no_mangle)]
pub fn main() {

    log::set_logger(&LOGGER).map(|_| log::set_max_level(log::LevelFilter::Debug));

    let args: Vec<String> = args().collect();

    let Some(command) = args.get(1) else { return; };

    match command.as_str() {
        "start" => {
            let Some(name) = args.get(2) else { return; };
            let mut service_manager = service_manager().lock();
            let res = service_manager.start(name.as_str());
            if let Err(e) = res {
                println!("{}", e);
            } else {
                println!("Successfully started service {}", name);
            }
        },
        "stop" => {
            let Some(name) = args.get(2) else { return; };
            let mut service_manager = service_manager().lock();
            let res = service_manager.stop(name.as_str());
            if let Err(e) = res {
                println!("{}", e);
            } else {
                println!("Successfully stopped service {}", name);
            }
        },
        "list" => {
            let service_manager = service_manager().lock();
            let services = service_manager.list();
            for svc in services {
                println!("{}:  {}", svc.name, svc.process.id())
            }
        }
        _ => {},
    }
}
