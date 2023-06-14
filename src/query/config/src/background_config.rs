use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;

use clap::Args;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::background::BackgroundJobParams;
use common_meta_app::background::BackgroundJobType;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Default, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundConfig {
    #[clap(long = "enable-background-service")]
    pub enable: bool,
    // Fs compaction related background config.
    #[clap(flatten)]
    pub compaction: BackgroundCompactionConfig,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundCompactionConfig {
    #[clap(long, default_value = "one_shot")]
    pub compact_mode: String,

    // Compact segments if a table has too many small segments
    // `segment_limit` is the maximum number of segments that would be compacted in a batch
    // None represent their is no limit
    // Details: https://databend.rs/doc/sql-commands/ddl/table/optimize-table#segment-compaction
    #[clap(long)]
    pub segment_limit: Option<u64>,

    // Compact small blocks into large one.
    // `block_limit` is the maximum number of blocks that would be compacted in a batch
    // None represent their is no limit
    // Details: https://databend.rs/doc/sql-commands/ddl/table/optimize-table#block-compaction
    #[clap(long)]
    pub block_limit: Option<u64>,

    #[clap(flatten)]
    pub scheduled_config: BackgroundScheduledConfig,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundScheduledConfig {
    // the fixed interval for compaction on each table.
    #[clap(long, default_value = "1800")]
    pub duration_secs: u64,

    // the cron expression for scheduled job,
    // by default it is scheduled with UTC timezone
    #[clap(long, default_value = "")]
    pub cron: String,

    #[clap(long)]
    pub time_zone: Option<String>,
}

impl BackgroundScheduledConfig {
    pub fn new_interval_job(duration_secs: u64) -> Self {
        Self {
            duration_secs,
            cron: "".to_string(),
            time_zone: None,
        }
    }

    pub fn new_cron_job(cron: String, time_zone: Option<String>) -> Self {
        Self {
            duration_secs: 0,
            cron,
            time_zone,
        }
    }
}

/// Config for background config
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InnerBackgroundConfig {
    pub enable: bool,
    pub compaction: InnerBackgroundCompactionConfig,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InnerBackgroundCompactionConfig {
    pub segment_limit: Option<u64>,
    pub block_limit: Option<u64>,
    pub params: BackgroundJobParams,
}

impl TryInto<InnerBackgroundConfig> for BackgroundConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerBackgroundConfig> {
        Ok(InnerBackgroundConfig {
            enable: self.enable,
            compaction: self.compaction.try_into()?,
        })
    }
}

impl From<InnerBackgroundConfig> for BackgroundConfig {
    fn from(inner: InnerBackgroundConfig) -> Self {
        Self {
            enable: inner.enable,
            compaction: BackgroundCompactionConfig::from(inner.compaction),
        }
    }
}

impl TryInto<InnerBackgroundCompactionConfig> for BackgroundCompactionConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerBackgroundCompactionConfig> {
        Ok(InnerBackgroundCompactionConfig {
            segment_limit: self.segment_limit,
            block_limit: self.block_limit,
            params: {
                match self.compact_mode.as_str() {
                    "one_shot" => BackgroundJobParams::new_one_shot_job(),
                    "interval" => BackgroundJobParams::new_interval_job(
                        std::time::Duration::from_secs(self.scheduled_config.duration_secs),
                    ),
                    "cron" => {
                        if self.scheduled_config.cron.is_empty() {
                            return Err(ErrorCode::InvalidArgument(
                                "cron expression is empty".to_string(),
                            ));
                        }
                        let tz = self
                            .scheduled_config
                            .time_zone
                            .clone()
                            .map(|x| chrono_tz::Tz::from_str(&x))
                            .transpose()
                            .map_err(|e| {
                                ErrorCode::InvalidArgument(format!("invalid time_zone: {}", e))
                            })?;
                        BackgroundJobParams::new_cron_job(self.scheduled_config.cron, tz)
                    }

                    _ => {
                        return Err(ErrorCode::InvalidArgument(format!(
                            "invalid compact_mode: {}",
                            self.compact_mode
                        )));
                    }
                }
            },
        })
    }
}

impl From<InnerBackgroundCompactionConfig> for BackgroundCompactionConfig {
    fn from(inner: InnerBackgroundCompactionConfig) -> Self {
        let mut cfg = Self {
            compact_mode: "".to_string(),
            segment_limit: inner.segment_limit,
            block_limit: inner.block_limit,
            scheduled_config: Default::default(),
        };
        match inner.params.job_type {
            BackgroundJobType::ONESHOT => {
                cfg.compact_mode = "one_shot".to_string();
            }
            BackgroundJobType::INTERVAL => {
                cfg.compact_mode = "interval".to_string();
                cfg.scheduled_config = BackgroundScheduledConfig::new_interval_job(
                    inner.params.scheduled_job_interval.as_secs(),
                );
            }
            BackgroundJobType::CRON => {
                cfg.compact_mode = "cron".to_string();
                cfg.scheduled_config = BackgroundScheduledConfig::new_cron_job(
                    inner.params.scheduled_job_cron,
                    inner.params.scheduled_job_timezone.map(|x| x.to_string()),
                );
            }
        }
        cfg
    }
}

impl From<BackgroundJobParams> for BackgroundScheduledConfig {
    fn from(inner: BackgroundJobParams) -> Self {
        Self {
            duration_secs: inner.scheduled_job_interval.as_secs(),
            cron: inner.scheduled_job_cron.clone(),
            time_zone: inner.scheduled_job_timezone.map(|x| x.to_string()),
        }
    }
}

impl Default for BackgroundCompactionConfig {
    fn default() -> Self {
        Self {
            compact_mode: "one_shot".to_string(),
            segment_limit: None,
            block_limit: None,
            scheduled_config: Default::default(),
        }
    }
}

impl Debug for BackgroundCompactionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackgroundCompactionConfig")
            .field("mode", &self.compact_mode)
            .field("segment_limit", &self.segment_limit)
            .field("block_limit", &self.block_limit)
            .field("fixed_config", &self.scheduled_config)
            .finish()
    }
}

impl Default for BackgroundScheduledConfig {
    fn default() -> Self {
        Self {
            duration_secs: 1800,
            cron: "".to_string(),
            time_zone: None,
        }
    }
}

impl Debug for BackgroundScheduledConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackgroundCompactionFixedConfig")
            .field("duration_secs", &self.duration_secs)
            .finish()
    }
}

impl Default for InnerBackgroundConfig {
    fn default() -> Self {
        Self {
            enable: false,
            compaction: InnerBackgroundCompactionConfig {
                segment_limit: None,
                block_limit: None,
                params: Default::default(),
            },
        }
    }
}

impl Debug for InnerBackgroundConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerBackgroundConfig")
            .field("compaction", &self.compaction)
            .finish()
    }
}

impl Debug for InnerBackgroundCompactionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerBackgroundCompactionConfig")
            .field("segment_limit", &self.segment_limit)
            .field("block_limit", &self.block_limit)
            .field("params", &self.params)
            .finish()
    }
}
