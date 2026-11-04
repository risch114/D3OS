use crate::speaker;



pub extern "sysv64" fn sys_speaker_play(frequency: usize, duration: usize) -> isize {
    speaker().play(frequency, duration);
    0
}