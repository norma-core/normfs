use clap::Parser;
use normfs::{CloudSettings, NormFS, NormFsSettings};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(unix)]
fn setup_ulimits() -> Result<(), Box<dyn std::error::Error>> {
    use libc::{getrlimit, setrlimit, rlimit, RLIMIT_NOFILE};
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

/// NormFS TCP Server - A standalone TCP server for NormFS
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// TCP address to listen on
    #[arg(short, long, default_value = "0.0.0.0:8888")]
    addr: String,

    /// Base folder for normfs storage
    #[arg(short, long, default_value = "./normfs_data")]
    data_dir: PathBuf,

    /// Maximum queue disk size in bytes (default: 32GB)
    #[arg(long, default_value = "34359738368")]
    max_queue_disk_size: u64,

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

    setup_ulimits()?;

    let args = Args::parse();

    log::info!("NormFS TCP Server starting...");
    log::info!("TCP address: {}", args.addr);
    log::info!("Data directory: {:?}", args.data_dir);
    log::info!("Max queue disk size: {} bytes", args.max_queue_disk_size);

    std::fs::create_dir_all(&args.data_dir)?;

    let mut settings = NormFsSettings {
        max_disk_usage_per_queue: Some(args.max_queue_disk_size),
        ..Default::default()
    };

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

    // Handle Ctrl+C for graceful shutdown
    let normfs_for_shutdown = normfs.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        log::info!("Received Ctrl+C, shutting down...");
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
