pub extern "sysv64" fn sys_port_out(port: usize, value: usize) -> isize {
    unsafe { x86_64::instructions::port::PortWrite::write_to_port(port as u16, value as u8) };
    0
}

pub extern "sysv64" fn sys_port_in(port: usize) -> isize {
    let value: u8 = unsafe { x86_64::instructions::port::PortRead::read_from_port(port as u16) };
    value as isize
}