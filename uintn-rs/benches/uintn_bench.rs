use criterion::{Criterion, black_box, criterion_group, criterion_main};
use dashu_int::UBig;
use uintn::UintN;

fn bench_increment_uintn(c: &mut Criterion) {
    let mut group = c.benchmark_group("increment");

    // Benchmark UintN increment for different sizes
    group.bench_function("uintn_u8", |b| {
        let mut val = UintN::from(200u8);
        b.iter(|| {
            val = black_box(val.increment());
        });
    });

    group.bench_function("uintn_u16", |b| {
        let mut val = UintN::from(60000u16);
        b.iter(|| {
            val = black_box(val.increment());
        });
    });

    group.bench_function("uintn_u32", |b| {
        let mut val = UintN::from(4_000_000_000u32);
        b.iter(|| {
            val = black_box(val.increment());
        });
    });

    group.bench_function("uintn_u64", |b| {
        let mut val = UintN::from(18_000_000_000_000_000_000u64);
        b.iter(|| {
            val = black_box(val.increment());
        });
    });

    group.bench_function("uintn_u128", |b| {
        let mut val = UintN::from(340_000_000_000_000_000_000_000_000_000_000_000_000u128);
        b.iter(|| {
            val = black_box(val.increment());
        });
    });

    // Benchmark dashu_int::UBig increment
    group.bench_function("dashu_ubig_small", |b| {
        let mut val = UBig::from(200u32);
        b.iter(|| {
            val += 1u32;
            black_box(&val);
        });
    });

    group.bench_function("dashu_ubig_medium", |b| {
        let mut val = UBig::from(4_000_000_000u32);
        b.iter(|| {
            val += 1u32;
            black_box(&val);
        });
    });

    group.bench_function("dashu_ubig_large", |b| {
        let mut val = UBig::from(18_000_000_000_000_000_000u64);
        b.iter(|| {
            val += 1u32;
            black_box(&val);
        });
    });

    group.finish();
}

fn bench_add_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("add");

    // Small numbers (fits in u8)
    group.bench_function("uintn_small", |b| {
        let a = UintN::from(100u8);
        let bb = UintN::from(50u8);
        b.iter(|| {
            black_box(a.add(&bb));
        });
    });

    group.bench_function("dashu_ubig_small", |b| {
        let a = UBig::from(100u32);
        let bb = UBig::from(50u32);
        b.iter(|| {
            black_box(&a + &bb);
        });
    });

    // Medium numbers (fits in u32)
    group.bench_function("uintn_medium", |b| {
        let a = UintN::from(2_000_000_000u32);
        let bb = UintN::from(1_500_000_000u32);
        b.iter(|| {
            black_box(a.add(&bb));
        });
    });

    group.bench_function("dashu_ubig_medium", |b| {
        let a = UBig::from(2_000_000_000u32);
        let bb = UBig::from(1_500_000_000u32);
        b.iter(|| {
            black_box(&a + &bb);
        });
    });

    // Large numbers (fits in u64)
    group.bench_function("uintn_large", |b| {
        let a = UintN::from(9_000_000_000_000_000_000u64);
        let bb = UintN::from(8_000_000_000_000_000_000u64);
        b.iter(|| {
            black_box(a.add(&bb));
        });
    });

    group.bench_function("dashu_ubig_large", |b| {
        let a = UBig::from(9_000_000_000_000_000_000u64);
        let bb = UBig::from(8_000_000_000_000_000_000u64);
        b.iter(|| {
            black_box(&a + &bb);
        });
    });

    // Very large numbers (requires u128 or Big)
    group.bench_function("uintn_very_large", |b| {
        let a = UintN::from(u128::MAX / 2);
        let bb = UintN::from(u128::MAX / 3);
        b.iter(|| {
            black_box(a.add(&bb));
        });
    });

    group.bench_function("dashu_ubig_very_large", |b| {
        let a = UBig::from(u128::MAX / 2);
        let bb = UBig::from(u128::MAX / 3);
        b.iter(|| {
            black_box(&a + &bb);
        });
    });

    group.finish();
}

fn bench_comparison_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison");

    // Same type comparisons
    group.bench_function("uintn_same_type", |b| {
        let a = UintN::from(1000u32);
        let bb = UintN::from(2000u32);
        b.iter(|| {
            black_box(a < bb);
        });
    });

    group.bench_function("dashu_ubig_same_size", |b| {
        let a = UBig::from(1000u32);
        let bb = UBig::from(2000u32);
        b.iter(|| {
            black_box(a < bb);
        });
    });

    // Cross-type comparisons (UintN only)
    group.bench_function("uintn_cross_type", |b| {
        let a = UintN::from(255u8);
        let bb = UintN::from(255u16);
        b.iter(|| {
            black_box(a == bb);
        });
    });

    // Large number comparisons
    group.bench_function("uintn_large_cmp", |b| {
        let a = UintN::from(u64::MAX - 1000);
        let bb = UintN::from(u64::MAX - 500);
        b.iter(|| {
            black_box(a < bb);
        });
    });

    group.bench_function("dashu_ubig_large_cmp", |b| {
        let a = UBig::from(u64::MAX - 1000);
        let bb = UBig::from(u64::MAX - 500);
        b.iter(|| {
            black_box(a < bb);
        });
    });

    group.finish();
}

fn bench_forever_incrementing_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("forever_incrementing_id");

    // Simulate a forever-incrementing ID with UintN
    group.bench_function("uintn_1000_increments", |b| {
        b.iter(|| {
            let mut id = UintN::zero();
            for _ in 0..1000 {
                id = black_box(id.increment());
            }
            id
        });
    });

    // Simulate with dashu_int::UBig
    group.bench_function("dashu_ubig_1000_increments", |b| {
        b.iter(|| {
            let mut id = UBig::from(0u32);
            for _ in 0..1000 {
                id += 1u32;
                black_box(&id);
            }
            id
        });
    });

    // Test with boundary crossings
    group.bench_function("uintn_with_transitions", |b| {
        b.iter(|| {
            let mut id = UintN::from(250u8);
            for _ in 0..20 {
                id = black_box(id.increment());
            }
            id
        });
    });

    group.finish();
}

fn bench_type_transitions(c: &mut Criterion) {
    let mut group = c.benchmark_group("type_transitions");

    // Benchmark transitions at type boundaries
    group.bench_function("u8_to_u16_transition", |b| {
        b.iter(|| {
            let val = UintN::from(255u8);
            black_box(val.increment())
        });
    });

    group.bench_function("u16_to_u32_transition", |b| {
        b.iter(|| {
            let val = UintN::from(65535u16);
            black_box(val.increment())
        });
    });

    group.bench_function("u32_to_u64_transition", |b| {
        b.iter(|| {
            let val = UintN::from(u32::MAX);
            black_box(val.increment())
        });
    });

    group.bench_function("u64_to_u128_transition", |b| {
        b.iter(|| {
            let val = UintN::from(u64::MAX);
            black_box(val.increment())
        });
    });

    group.bench_function("u128_to_big_transition", |b| {
        b.iter(|| {
            let val = UintN::from(u128::MAX);
            black_box(val.increment())
        });
    });

    group.finish();
}

fn bench_memory_and_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory");

    // Creation benchmarks
    group.bench_function("uintn_create_u8", |b| {
        b.iter(|| {
            black_box(UintN::from(42u8));
        });
    });

    group.bench_function("uintn_create_u64", |b| {
        b.iter(|| {
            black_box(UintN::from(42u64));
        });
    });

    group.bench_function("uintn_create_u128", |b| {
        b.iter(|| {
            black_box(UintN::from(42u128));
        });
    });

    group.bench_function("dashu_ubig_create_small", |b| {
        b.iter(|| {
            black_box(UBig::from(42u32));
        });
    });

    group.bench_function("dashu_ubig_create_large", |b| {
        b.iter(|| {
            black_box(UBig::from(u128::MAX));
        });
    });

    // Clone benchmarks
    group.bench_function("uintn_clone_u8", |b| {
        let val = UintN::from(42u8);
        b.iter(|| {
            black_box(val.clone());
        });
    });

    group.bench_function("uintn_clone_u128", |b| {
        let val = UintN::from(u128::MAX);
        b.iter(|| {
            black_box(val.clone());
        });
    });

    group.bench_function("dashu_ubig_clone", |b| {
        let val = UBig::from(u128::MAX);
        b.iter(|| {
            black_box(val.clone());
        });
    });

    group.finish();
}

fn bench_shrink_to_fit(c: &mut Criterion) {
    let mut group = c.benchmark_group("shrink_to_fit");

    group.bench_function("from_u32_to_u8", |b| {
        let val = UintN::from(255u32);
        b.iter(|| {
            black_box(val.shrink_to_fit());
        });
    });

    group.bench_function("from_u64_to_u16", |b| {
        let val = UintN::from(65535u64);
        b.iter(|| {
            black_box(val.shrink_to_fit());
        });
    });

    group.bench_function("from_u128_to_u32", |b| {
        let val = UintN::from(4294967295u128);
        b.iter(|| {
            black_box(val.shrink_to_fit());
        });
    });

    group.bench_function("no_shrink_needed", |b| {
        let val = UintN::from(255u8);
        b.iter(|| {
            black_box(val.shrink_to_fit());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_increment_uintn,
    bench_add_operations,
    bench_comparison_operations,
    bench_forever_incrementing_id,
    bench_type_transitions,
    bench_memory_and_allocation,
    bench_shrink_to_fit
);
criterion_main!(benches);
