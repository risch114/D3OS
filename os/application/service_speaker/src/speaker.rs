#![allow(unused_must_use)]

use concurrent::thread;
use syscall::syscall;

const PIT_BASE_FREQ: usize = 1193182; // taken from kernel::pit

const CTRL_PORT: u16 = 0x43;
const DATA_PORT_2: u16 = 0x42;
const PPI_PORT: u16 = 0x61;

pub fn on(freq: usize) {
    
    let counter = PIT_BASE_FREQ / freq;

    syscall(syscall::SystemCall::PortOut, &[CTRL_PORT as usize, 0xB6]);
    syscall(syscall::SystemCall::PortOut, &[DATA_PORT_2 as usize, (counter % 256)]);
    syscall(syscall::SystemCall::PortOut, &[DATA_PORT_2 as usize, (counter / 256)]);
    
    // Turn speaker on
    let status = syscall(syscall::SystemCall::PortIn, &PPI_PORT).unwrap();
    syscall(syscall::SystemCall::PortOut, &[PPI_PORT as usize, status | 0x03]);
}

pub fn off() {
    let status = syscall(syscall::SystemCall::PortIn, &PPI_PORT).unwrap();
    syscall(syscall::SystemCall::PortOut, &[PPI_PORT as usize, status & 0xFC]);
}

pub fn play(freq: usize, duration_ms: usize) {
    on(freq);
    thread::sleep(duration_ms);
    off();
}
