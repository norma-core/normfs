use globset::{Glob, GlobMatcher};
use normfs_types::{CompressionType, EncryptionType};

#[derive(Debug, Clone, Copy)]
pub struct QueueConfig {
    pub compression_type: CompressionType,
    pub enable_fsync: bool,
    pub encryption_type: EncryptionType,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            compression_type: CompressionType::Zstd,
            enable_fsync: true,
            encryption_type: EncryptionType::Aes,
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
    ) -> Result<Self, globset::Error> {
        let rules = patterns
            .into_iter()
            .map(|(pat, config)| {
                let glob = Glob::new(&pat)?;
                Ok((glob.compile_matcher(), config))
            })
            .collect::<Result<Vec<_>, globset::Error>>()?;
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
}

#[derive(Default)]
pub struct QueueMode {
    pub readonly: bool,
}
