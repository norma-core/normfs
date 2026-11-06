use clap::{Parser, ValueEnum};
use normfs::{CloudSettings, NormFS, NormFsSettings, Persist, QueueSettings};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[path = "normfs-server/size.rs"]
mod size;
#[cfg(test)]
#[path = "normfs-server/size_test.rs"]
mod size_test;

#[cfg(unix)]
fn setup_ulimits() -> Result<(), Box<dyn std::error::Error>> {
    use libc::{getrlimit, rlimit, setrlimit, RLIMIT_NOFILE};
    use std::mem::MaybeUninit;

    unsafe {
        let mut rlim = MaybeUninit::<rlimit>::uninit();
        if getrlimit(RLIMIT_NOFILE, rlim.as_mut_ptr()) != 0 {
            return Err("Failed to get current RLIMIT_NOFILE".into());
        }

        let mut rlim = rlim.assume_init();
        rlim.rlim_cur = 256000;
        rlim.rlim_max = 256000;

        if setrlimit(RLIMIT_NOFILE, &rlim) != 0 {
            return Err("Failed to set RLIMIT_NOFILE to 256000".into());
        }

        log::info!("Set RLIMIT_NOFILE to 256000 for high load");
    }

    Ok(())
}

#[cfg(not(unix))]
fn setup_ulimits() -> Result<(), Box<dyn std::error::Error>> {
    log::warn!("RLIMIT_NOFILE configuration not supported on this platform");
    Ok(())
}

/// How records reach the local disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum PersistMode {
    /// Every record goes to the WAL; full WAL files are archived to the store.
    WalStore,
    /// Each full memory page is sealed straight into a store file, with no
    /// WAL: records in the open page are lost on a crash. A page is a file, so
    /// give it a few MiB with --mem-page-size.
    Store,
}

/// NormFS TCP Server - A standalone TCP server for NormFS
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None,
    after_help = "Sizes accept bytes or case-insensitive units: KiB/MiB/GiB/TiB/PiB/EiB (powers of 1024), KB/MB/GB/TB/PB/EB (powers of 1000). Fractions must resolve to whole bytes, e.g. 1.5GiB.")]
struct Args {
    /// TCP address to listen on
    #[arg(short, long, default_value = "0.0.0.0:8888")]
    addr: String,

    /// Base folder for normfs storage
    #[arg(short, long, default_value = "./normfs_data")]
    data_dir: PathBuf,

    /// Active page budget in bytes; excludes other process memory
    #[arg(long, default_value_t = NormFsSettings::default().max_memory_usage,
        value_parser = size::parse_memory)]
    max_memory_usage: usize,

    /// Active page size in bytes; a record plus framing must fit in one page
    #[arg(long, default_value_t = NormFsSettings::default().mem_page_size,
        value_parser = size::parse_memory)]
    mem_page_size: usize,

    /// Passive page budget in bytes, separate from the active budget
    #[arg(long, default_value_t = NormFsSettings::default().max_passive_memory_usage,
        value_parser = size::parse_memory)]
    max_passive_memory_usage: usize,

    /// Passive page size in bytes
    #[arg(long, default_value_t = NormFsSettings::default().mem_passive_page_size,
        value_parser = size::parse_memory)]
    mem_passive_page_size: usize,

    /// Per-queue WAL + store retention threshold in bytes
    #[arg(long, default_value = "32GiB", value_parser = size::parse_bytes)]
    max_queue_disk_size: u64,

    /// Disable disk retention limits and size-triggered deletion
    #[arg(long, conflicts_with = "max_queue_disk_size")]
    unlimited_disk: bool,

    /// How records reach the local disk
    #[arg(long, value_enum, default_value_t = PersistMode::WalStore)]
    persist: PersistMode,

    /// Store and cloud files reads may hold decoded at once [default: CPU count, 2 to 8]
    #[arg(long)]
    cold_read_files: Option<usize>,

    /// Files sealed for the store at once [default: CPU count / 4, at least 4]
    #[arg(long)]
    store_workers: Option<usize>,

    /// S3 bucket name for cloud offloading (optional)
    #[arg(long)]
    s3_bucket: Option<String>,

    /// S3 region (defaults to AWS_REGION env var)
    #[arg(long, env = "AWS_REGION")]
    s3_region: Option<String>,

    /// S3 endpoint URL (optional, for S3-compatible services)
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 access key ID (defaults to AWS_ACCESS_KEY_ID env var)
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    s3_access_key: Option<String>,

    /// S3 secret access key (defaults to AWS_SECRET_ACCESS_KEY env var)
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    s3_secret_key: Option<String>,

    /// S3 session token (defaults to AWS_SESSION_TOKEN env var, optional)
    #[arg(long, env = "AWS_SESSION_TOKEN")]
    s3_session_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    setup_ulimits()?;

    log::info!("NormFS TCP Server starting...");
    log::info!("TCP address: {}", args.addr);
    log::info!("Data directory: {:?}", args.data_dir);
    log::info!(
        "Active page budget: {} bytes, page size: {} bytes",
        args.max_memory_usage,
        args.mem_page_size
    );
    log::info!(
        "Passive page budget: {} bytes, page size: {} bytes",
        args.max_passive_memory_usage,
        args.mem_passive_page_size
    );
    if args.unlimited_disk {
        log::info!("Queue disk retention: unlimited");
    } else {
        log::info!("Max queue disk size: {} bytes", args.max_queue_disk_size);
    }

    // All-active until the server grows a way to declare per-queue pool
    // rules: a passive default without that knob would silently cap every
    // record at a passive page with no recourse from the command line.
    let persist = Persist {
        cloud: args.s3_bucket.is_some(),
        ..match args.persist {
            PersistMode::WalStore => Persist::WAL_STORE,
            PersistMode::Store => Persist::STORE,
        }
    };
    log::info!("Persist: {:?}", args.persist);
    let mut settings = NormFsSettings {
        max_memory_usage: args.max_memory_usage,
        mem_page_size: args.mem_page_size,
        max_passive_memory_usage: args.max_passive_memory_usage,
        mem_passive_page_size: args.mem_passive_page_size,
        max_disk_usage_per_queue: if args.unlimited_disk {
            None
        } else {
            Some(args.max_queue_disk_size)
        },
        queue_settings: QueueSettings::all_active().with_default_persist(persist),
        // A server reads many queues at once, each wanting its file and the
        // next; each slot can hold a whole WAL file decoded, so it stays capped.
        cold_read_files: args.cold_read_files.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(2)
                .clamp(2, 8)
        }),
        ..NormFsSettings::default()
    };
    log::info!("Cold read files: {}", settings.cold_read_files);
    // Sealing a page is zstd and AES on the CPU; the library's four slots kept
    // a 192-core host sealing on four cores while its pages ran out.
    settings.store_cfg.num_workers = args.store_workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get() / 4)
            .unwrap_or(0)
            .max(4)
    });
    log::info!("Store workers: {}", settings.store_cfg.num_workers);

    // Configure S3 cloud offloading if provided
    if let Some(bucket) = &args.s3_bucket {
        let region_str = args
            .s3_region
            .as_ref()
            .ok_or("S3 region is required - set AWS_REGION environment variable")?;

        let access_key = args
            .s3_access_key
            .as_ref()
            .ok_or("S3 access key is required - set AWS_ACCESS_KEY_ID environment variable")?;

        let secret_key = args.s3_secret_key.as_ref().ok_or(
            "S3 secret access key is required - set AWS_SECRET_ACCESS_KEY environment variable",
        )?;

        settings.cloud_settings = Some(CloudSettings {
            endpoint: args.s3_endpoint.clone().unwrap_or_default(),
            bucket: bucket.clone(),
            region: region_str.clone(),
            access_key: access_key.clone(),
            secret_key: secret_key.clone(),
            prefix: String::new(), // NormFS will use instance_id as prefix automatically
        });

        log::info!("Cloud offload enabled for bucket: {}", bucket);
    }

    let normfs = NormFS::new(args.data_dir.clone(), settings).await?;
    log::info!("NormFS instance ID: {}", normfs.get_instance_id());

    let normfs = Arc::new(normfs);

    let tcp_addr: SocketAddr = args
        .addr
        .parse()
        .or_else(|_| format!("0.0.0.0:{}", args.addr).parse())
        .map_err(|e| format!("Invalid address '{}': {}", args.addr, e))?;

    let server = normfs::server::Server::new(tcp_addr, normfs.clone()).await?;
    log::info!("NormFS TCP server listening on {}", tcp_addr);

    let normfs_for_shutdown = normfs.clone();
    tokio::spawn(async move {
        stop_signal().await;
        log::info!("Received a stop signal, shutting down...");
        log::info!("Closing NormFS (writing WAL)...");
        if let Err(e) = normfs_for_shutdown.close().await {
            log::error!("Error during NormFS close: {}", e);
        }
        log::info!("NormFS closed successfully");
        std::process::exit(0);
    });

    server.run().await?;

    Ok(())
}

/// Ctrl+C, or SIGTERM from a service manager: without a close, a store-only
/// queue loses its open page.
async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("Failed to listen for SIGTERM");
        tokio::select! {
            r = tokio::signal::ctrl_c() => r.expect("Failed to listen for Ctrl+C"),
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}
