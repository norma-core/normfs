use std::path::Path;

use crate::plan::{Kind, Op, Plan, PlanError, TmpMode};

#[test]
fn publish_walks_its_steps_and_refuses_bad_reports() {
    let mut p = Plan::publish(Path::new("t"), Path::new("d"), TmpMode::Excl, 10).unwrap();
    assert_eq!(p.kind(), Kind::Publish);
    assert_eq!(p.next().unwrap(), Op::Open);
    assert!(matches!(p.ok(0), Err(PlanError::State)));
    p.ok(77).unwrap();
    assert_eq!(p.next().unwrap(), Op::Write);
    assert!(matches!(p.ok(11), Err(PlanError::State)));
    p.ok(4).unwrap();
    assert_eq!(p.next().unwrap(), Op::Write);
    assert_eq!(p.written(), 4);
    p.ok(6).unwrap();
    for expected in [Op::FsyncFile, Op::CloseFile, Op::StatDst] {
        assert_eq!(p.next().unwrap(), expected);
        p.ok(0).unwrap();
    }
    assert_eq!(p.next().unwrap(), Op::Rename);
    assert_eq!(p.old_len(), Some(0));
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::FsyncDir);
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::Done);
    assert!(matches!(p.ok(0), Err(PlanError::State)));
    assert!(matches!(p.err(5), Err(PlanError::State)));
}

#[test]
fn publish_absent_only_at_stat() {
    let mut p = Plan::publish(Path::new("t"), Path::new("d"), TmpMode::Trunc, 1).unwrap();
    assert!(matches!(p.absent(), Err(PlanError::State)));
    p.ok(1).unwrap();
    p.ok(1).unwrap();
    p.ok(0).unwrap();
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::StatDst);
    p.absent().unwrap();
    assert_eq!(p.next().unwrap(), Op::Rename);
    assert_eq!(p.old_len(), None);
}

#[test]
fn publish_failure_ends_the_plan() {
    let mut p = Plan::publish(Path::new("t"), Path::new("d"), TmpMode::Excl, 3).unwrap();
    p.ok(9).unwrap();
    p.err(28).unwrap();
    assert_eq!(p.next().unwrap(), Op::Failed);
    assert_eq!(p.os_error(), 28);
}

#[test]
fn append_cuts_back_after_a_failure() {
    let mut p = Plan::append(Path::new("f"), 5, 100, 8).unwrap();
    assert_eq!(p.next().unwrap(), Op::Write);
    p.ok(8).unwrap();
    assert_eq!(p.next().unwrap(), Op::FsyncFile);
    p.err(5).unwrap();
    assert_eq!(p.next().unwrap(), Op::TruncateBack);
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::Failed);
    assert!(p.restored());
    assert_eq!(p.os_error(), 5);

    let mut p = Plan::append(Path::new("f"), 5, 100, 8).unwrap();
    p.err(28).unwrap();
    assert_eq!(p.next().unwrap(), Op::TruncateBack);
    p.err(5).unwrap();
    assert_eq!(p.next().unwrap(), Op::Failed);
    assert!(!p.restored());
    assert_eq!(p.os_error(), 28);
}

#[test]
fn append_commits_only_after_fsync() {
    let mut p = Plan::append(Path::new("f"), 5, 100, 8).unwrap();
    p.ok(3).unwrap();
    p.ok(5).unwrap();
    assert_eq!(p.next().unwrap(), Op::FsyncFile);
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::Done);
}

#[test]
fn restore_is_one_step() {
    let mut p = Plan::restore(Path::new("f"), 5, 100).unwrap();
    assert_eq!(p.next().unwrap(), Op::TruncateBack);
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::Done);
    let mut p = Plan::restore(Path::new("f"), 5, 100).unwrap();
    p.err(5).unwrap();
    assert_eq!(p.next().unwrap(), Op::Failed);
}

#[test]
fn create_skips_write_for_an_empty_file() {
    let mut p = Plan::create(Path::new("m"), TmpMode::Trunc, 0).unwrap();
    p.ok(4).unwrap();
    assert_eq!(p.next().unwrap(), Op::FsyncFile);
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::FsyncDir);
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::Done);

    let mut p = Plan::create(Path::new("m"), TmpMode::Excl, 2).unwrap();
    p.ok(4).unwrap();
    assert_eq!(p.next().unwrap(), Op::Write);
}

#[test]
fn remove_treats_absent_as_removed() {
    let mut p = Plan::remove(Path::new("m")).unwrap();
    assert_eq!(p.next().unwrap(), Op::Unlink);
    p.absent().unwrap();
    assert_eq!(p.next().unwrap(), Op::FsyncDir);
    p.ok(0).unwrap();
    assert_eq!(p.next().unwrap(), Op::Done);
}

#[test]
fn interior_nul_is_refused() {
    assert!(matches!(
        Plan::remove(Path::new("a\0b")),
        Err(PlanError::Path)
    ));
}
