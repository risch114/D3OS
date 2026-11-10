use spin::Mutex;

use crate::const_string::ConstString;


pub struct SlotMap<'a, T: Default, const S: usize> {
    data: [T; S],
    slots: Mutex<[Option<Slot<'a, T>>; S]>,
}

struct Slot<'a, T: Default> {
    name: ConstString<64>,
    index: usize,
    slot_ref: &'a T,
}

impl<'a, T: Default, const S: usize> SlotMap<'a, T, S> {

    pub fn new() -> Self {
        Self { data: core::array::from_fn(|_| T::default()), slots: Mutex::new(core::array::from_fn(|_| None)) }
    }

    /// find the element with `name`
    pub fn find(&self, name: &str) -> Option<&'a T> {
        let idx = self.index(name)?;
        let slot_ref = self.slots.lock()[idx].as_ref()?.slot_ref;
        Some(slot_ref)
    }

    /// find the index of the element with `name`
    pub fn index(&self, name: &str) -> Option<usize> {
        self.slots.lock().iter()
            .filter_map(|opt| opt.as_ref())
            .find(|slot| slot.name == name).map(|slot| slot.index)
    }

    fn next_free_idx(&self) -> Option<usize> {
        for (idx, slot) in self.slots.lock().iter().enumerate() {
            if slot.is_none() {
                return Some(idx);
            }
        }
        None
    }

    /// mark the next free slot as occupied and return the referenced data \
    /// if there already exists a slot with `name`, return the existing data instead
    pub fn occupy_slot(&self, name: &str) -> Option<&'a T> {

        let l = self.slots.lock().len();

        if l == 0 {
            return unsafe { self.register_idx(name, 0) };
        }

        // service already exists, return it
        if let Some(r) = self.find(name) {
            return Some(r);
        }

        // maximum number of services reached
        if l >= S {
            return None;
        }
        
        match self.next_free_idx() {
            Some(idx) => unsafe { self.register_idx(name, idx) },
            None => None,
        }
    }

    /// free the slot with `name`, without clearing the underlying data
    pub fn free_slot(&self, name: &str) -> bool {

        let l = self.slots.lock().len();

        if l == 0 {
            return false;
        }
        
        let found_idx = self.index(name);

        match found_idx {
            Some(idx) => {
                self.slots.lock()[idx] = None;
                true
            },
            None => false,
        }
    }

    /// free the slot with `name`, and clear (null) the underlying data
    /// 
    /// ## Safety
    /// this function clears the data using `ptr::write` \
    /// this is unsafe if the data is still in use
    pub unsafe fn clear_slot(&self, name: &str) -> bool {

        let l = self.slots.lock().len();

        if l == 0 {
            return false;
        }
        
        let found_idx = self.index(name);

        match found_idx {
            Some(idx) => {
                unsafe {
                    let ptr = (self.data.as_ptr() as *mut T).add(idx);
                    ptr.write(T::default());
                }
                self.slots.lock()[idx] = None;
                true
            },
            None => false,
        }
    }

    unsafe fn register_idx(&self, name: &str, idx: usize) -> Option<&'a T> {
        let slot_ref = unsafe { &*self.data.as_ptr().add(idx) };
        let slot = Slot {
            name: name.into(),
            index: idx,
            slot_ref,
        };
        self.slots.lock()[idx] = Some(slot);
        Some(slot_ref)
    }
}

impl<'a, T: Default, const S: usize> Default for SlotMap<'a, T, S> {
    fn default() -> Self {
        Self::new()
    }
}