#![no_std]

pub mod const_string;

/// copy `(dst, src)`, but automatically adjust for differing sizes \
/// returns the actual length which was copied (i.e. `min(dst.len, src.len)`)
#[macro_export] macro_rules! copy_from_slice_sizechecked {
    ($dst:expr, $src:expr) => {
        {
            let l = core::cmp::min($dst.len(), $src.len());
            $dst[..l].copy_from_slice(&$src[..l]);
            l
        }
    };
    ($dst:expr, $src:expr, $amount:expr) => {
        {
            let l = core::cmp::min(core::cmp::min($dst.len(), $src.len()), $amount);
            $dst[..l].copy_from_slice(&$src[..l]);
            l
        }
    };
}

/// allocate a new array on the stack, initialized from the data of the given `src` slice \
/// the `src` slice data is copied sizechecked
#[macro_export] macro_rules! array_from_slice {
    ($src:expr) => {
        array_from_slice!($src, _, _)
    };
    ($src:expr, $type:ty) => {
        array_from_slice!($src, $type, _)
    };
    ($src:expr, $type:ty, $size:expr) => {
        {
            let mut arr = [<$type>::default(); $size];
            copy_from_slice_sizechecked!(arr, $src);
            arr
        }
    };
}

#[macro_export] macro_rules! any_as_bytes {
    ($any_ref:expr) => {
        core::slice::from_raw_parts(core::ptr::from_ref($any_ref) as *const u8, core::mem::size_of_val($any_ref))
    };
}

#[macro_export] macro_rules! bytes_as_any {
    ($bytes:expr) => {
        &*($bytes.as_ptr() as *const _)
    };
    ($bytes:expr; $any_type:ty) => {
        {
            debug_assert!($bytes.len() == core::mem::size_of::<$any_type>());
            &*($bytes.as_ptr() as *const $any_type)
        }
    }
}