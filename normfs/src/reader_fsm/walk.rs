use std::os::raw::c_int;

use uintn::UintN;

// Mirrors c/include/normfs/walk.h.
const NORMFS_WALK_READ: c_int = 0;
const NORMFS_WALK_BACK: c_int = 1;
const NORMFS_WALK_GAP: c_int = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalkStep {
    Read,
    Back,
    Gap,
    UnknownStatus(c_int),
}

unsafe extern "C" {
    fn normfs_walk_step(
        file_id: u64,
        begins_at: u64,
        next_id: u64,
        has_cut_short: c_int,
        cut_short: u64,
        has_went_back: c_int,
        went_back: u64,
    ) -> c_int;
}

/// Ids past u64 never go back; warn_gap still reports a hole below them.
pub(crate) fn walk_step(
    file_id: &UintN,
    begins_at: &UintN,
    next_id: &UintN,
    cut_short: Option<&UintN>,
    went_back: Option<&UintN>,
) -> WalkStep {
    let (Ok(file_id), Ok(begins_at), Ok(next_id)) =
        (file_id.to_u64(), begins_at.to_u64(), next_id.to_u64())
    else {
        return WalkStep::Read;
    };
    let cut_short = cut_short.and_then(|id| id.to_u64().ok());
    let went_back = went_back.and_then(|id| id.to_u64().ok());
    // SAFETY: plain integers in and out; the C reads no memory.
    let status = unsafe {
        normfs_walk_step(
            file_id,
            begins_at,
            next_id,
            cut_short.is_some() as c_int,
            cut_short.unwrap_or(0),
            went_back.is_some() as c_int,
            went_back.unwrap_or(0),
        )
    };
    match status {
        NORMFS_WALK_READ => WalkStep::Read,
        NORMFS_WALK_BACK => WalkStep::Back,
        NORMFS_WALK_GAP => WalkStep::Gap,
        other => WalkStep::UnknownStatus(other),
    }
}
