use globset::{Glob, GlobMatcher};
use normfs_types::{CompressionType, EncryptionType};

/// The stages a record passes through after memory. Each is optional, per
/// queue: `memory -> [wal] -> [store] -> [cloud]`.
///
/// With `wal`, records reach a `.wal` file within `write_interval` and the
/// store migrates each full file. Without it but with `store`, a sealed memory
/// page becomes one store file directly: no timer, so a crash loses at most
/// the open page. With `cloud` alone the sealed page becomes one object in
/// the bucket and nothing touches the local disk but the pointer that names
/// the last file. With none of the three, the queue lives in memory and only
/// its last id survives a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Persist {
    pub wal: bool,
    pub store: bool,
    /// Store files go to the configured bucket: offloaded from the local
    /// store when there is one, landed there directly when there is not.
    pub cloud: bool,
}

impl Persist {
    pub const MEMORY: Self = Self {
        wal: false,
        store: false,
        cloud: false,
    };
    pub const WAL_STORE: Self = Self {
        wal: true,
        store: true,
        cloud: false,
    };
    pub const STORE: Self = Self {
        wal: false,
        store: true,
        cloud: false,
    };
    pub const CLOUD: Self = Self {
        wal: false,
        store: false,
        cloud: true,
    };

    pub fn is_memory(self) -> bool {
        !self.wal && !self.store && !self.cloud
    }

    /// Who takes pages out of this queue's pool.
    pub fn drainer(self) -> Drainer {
        if self.wal {
            Drainer::Wal
        } else if self.store || self.cloud {
            Drainer::Page
        } else {
            Drainer::None
        }
    }

    fn validate(self, pattern: &str) -> Result<(), ConfigError> {
        let pattern = pattern.to_string();
        if self.wal && !self.store {
            return Err(ConfigError::WalWithoutStore { pattern });
        }
        Ok(())
    }
}

impl Default for Persist {
    fn default() -> Self {
        Self::WAL_STORE
    }
}

/// What drains a queue's pool, which is what every write-side branch asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drainer {
    /// Nothing: full pages are evicted, oldest first.
    None,
    /// The WAL file writer, from a per-record channel.
    Wal,
    /// A page-per-file writer, straight from the sealed page.
    Page,
}

#[derive(Debug)]
pub enum ConfigError {
    Glob(globset::Error),
    /// Only the store migration ever consumes a `.wal`, so without a store
    /// the files would pile up and nothing would archive or delete them.
    WalWithoutStore {
        pattern: String,
    },
    /// A rule asks for cloud and the instance has none. An error, not a
    /// warning: the disk monitor would delete what was never sent.
    CloudWithoutSettings {
        pattern: String,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Glob(e) => write!(f, "invalid queue pattern: {e}"),
            ConfigError::WalWithoutStore { pattern } => {
                write!(
                    f,
                    "rule '{pattern}': wal without store, nothing would archive the files"
                )
            }
            ConfigError::CloudWithoutSettings { pattern } => {
                write!(
                    f,
                    "rule '{pattern}': cloud requested but no cloud settings configured"
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Glob(e) => Some(e),
            _ => None,
        }
    }
}

impl From<globset::Error> for ConfigError {
    fn from(e: globset::Error) -> Self {
        ConfigError::Glob(e)
    }
}

/// Which arena a queue draws pages from. The pool sets the page size, and
/// with it the idle 2-page floor and the widest record the queue accepts.
/// Passive by default: wide records are for queues somebody names in a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoolKind {
    Active,
    #[default]
    Passive,
}

#[derive(Debug, Clone, Copy)]
pub struct QueueConfig {
    pub compression_type: CompressionType,
    pub enable_fsync: bool,
    pub encryption_type: EncryptionType,
    pub pool: PoolKind,
    pub persist: Persist,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            compression_type: CompressionType::Zstd,
            enable_fsync: true,
            encryption_type: EncryptionType::Aes,
            pool: PoolKind::default(),
            persist: Persist::default(),
        }
    }
}

impl QueueConfig {
    pub fn active() -> Self {
        Self {
            pool: PoolKind::Active,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueueSettings {
    rules: Vec<(GlobMatcher, QueueConfig)>,
    pub default_config: QueueConfig,
}

impl QueueSettings {
    pub fn new(
        patterns: Vec<(String, QueueConfig)>,
        default_config: QueueConfig,
    ) -> Result<Self, ConfigError> {
        default_config.persist.validate("default")?;
        let rules = patterns
            .into_iter()
            .map(|(pat, config)| {
                config.persist.validate(&pat)?;
                let glob = Glob::new(&pat)?;
                Ok((glob.compile_matcher(), config))
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        Ok(Self {
            rules,
            default_config,
        })
    }

    pub fn get_config(&self, queue_path: &str) -> QueueConfig {
        for (matcher, config) in &self.rules {
            if matcher.is_match(queue_path) {
                return *config;
            }
        }
        self.default_config
    }

    pub fn all_active() -> Self {
        Self {
            rules: Vec::new(),
            default_config: QueueConfig::active(),
        }
    }

    /// The same rules with the default queue's pipeline replaced.
    pub fn with_default_persist(mut self, persist: Persist) -> Self {
        self.default_config.persist = persist;
        self
    }

    /// Every rule that asks for cloud, by pattern. `NormFS::new` refuses an
    /// instance where this is non-empty and no cloud is configured.
    pub fn cloud_rules(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .rules
            .iter()
            .filter(|(_, c)| c.persist.cloud)
            .map(|(m, _)| m.glob().glob().to_string())
            .collect();
        if self.default_config.persist.cloud {
            out.push("default".to_string());
        }
        out
    }
}

#[derive(Default)]
pub struct QueueMode {
    pub readonly: bool,
}
