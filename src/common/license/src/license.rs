// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ComputeQuota {
    threads_num: Option<usize>,
    memory_usage: Option<usize>,
}

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClusterQuota {
    pub(crate) max_clusters: Option<usize>,
    pub(crate) max_nodes_per_cluster: Option<usize>,
}

impl ClusterQuota {
    pub fn un_limit() -> ClusterQuota {
        ClusterQuota {
            max_clusters: None,
            max_nodes_per_cluster: None,
        }
    }

    pub fn limit_clusters(max_clusters: usize) -> ClusterQuota {
        ClusterQuota {
            max_nodes_per_cluster: None,
            max_clusters: Some(max_clusters),
        }
    }

    pub fn limit_nodes(nodes: usize) -> ClusterQuota {
        ClusterQuota {
            max_clusters: None,
            max_nodes_per_cluster: Some(nodes),
        }
    }

    pub fn limit_full(max_clusters: usize, nodes: usize) -> ClusterQuota {
        ClusterQuota {
            max_clusters: Some(max_clusters),
            max_nodes_per_cluster: Some(nodes),
        }
    }
}

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StorageQuota {
    pub storage_usage: Option<usize>,
}

/// We allow user to use upto 1TiB storage size.
impl Default for StorageQuota {
    fn default() -> Self {
        Self {
            storage_usage: Some(1024 * 1024 * 1024 * 1024),
        }
    }
}

// All enterprise features are defined here.
#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Feature {
    #[serde(alias = "license_info", alias = "LICENSE_INFO")]
    LicenseInfo,
    #[serde(alias = "vacuum", alias = "VACUUM")]
    Vacuum,
    #[serde(alias = "test", alias = "TEST")]
    Test,
    #[serde(alias = "virtual_column", alias = "VIRTUAL_COLUMN")]
    VirtualColumn,
    #[serde(alias = "background_service", alias = "BACKGROUND_SERVICE")]
    BackgroundService,
    #[serde(alias = "data_mask", alias = "DATA_MASK")]
    DataMask,
    #[serde(alias = "aggregate_index", alias = "AGGREGATE_INDEX")]
    AggregateIndex,
    #[serde(alias = "inverted_index", alias = "INVERTED_INDEX")]
    InvertedIndex,
    #[serde(alias = "computed_column", alias = "COMPUTED_COLUMN")]
    ComputedColumn,
    #[serde(alias = "storage_encryption", alias = "STORAGE_ENCRYPTION")]
    StorageEncryption,
    #[serde(alias = "stream", alias = "STREAM")]
    Stream,
    #[serde(alias = "attach_table", alias = "ATTACH_TABLE")]
    AttacheTable,
    #[serde(alias = "compute_quota", alias = "COMPUTE_QUOTA")]
    ComputeQuota(ComputeQuota),
    #[serde(alias = "storage_quota", alias = "STORAGE_QUOTA")]
    StorageQuota(StorageQuota),
    #[serde(alias = "cluster_quota", alias = "CLUSTER_QUOTA")]
    ClusterQuota(ClusterQuota),
    #[serde(alias = "amend_table", alias = "AMEND_TABLE")]
    AmendTable,
    #[serde(other)]
    Unknown,
}

impl Display for Feature {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Feature::LicenseInfo => write!(f, "license_info"),
            Feature::Vacuum => write!(f, "vacuum"),
            Feature::Test => write!(f, "test"),
            Feature::VirtualColumn => write!(f, "virtual_column"),
            Feature::BackgroundService => write!(f, "background_service"),
            Feature::DataMask => write!(f, "data_mask"),
            Feature::AggregateIndex => write!(f, "aggregate_index"),
            Feature::InvertedIndex => write!(f, "inverted_index"),
            Feature::ComputedColumn => write!(f, "computed_column"),
            Feature::StorageEncryption => write!(f, "storage_encryption"),
            Feature::Stream => write!(f, "stream"),
            Feature::AttacheTable => write!(f, "attach_table"),
            Feature::ComputeQuota(v) => {
                write!(f, "compute_quota(")?;

                match &v.threads_num {
                    None => write!(f, "threads_num: unlimited,")?,
                    Some(threads_num) => write!(f, "threads_num: {}", *threads_num)?,
                };

                match v.memory_usage {
                    None => write!(f, "memory_usage: unlimited,")?,
                    Some(memory_usage) => write!(f, "memory_usage: {}", memory_usage)?,
                };

                write!(f, ")")
            }
            Feature::StorageQuota(v) => {
                write!(f, "storage_quota(")?;

                match v.storage_usage {
                    None => write!(f, "storage_usage: unlimited,")?,
                    Some(storage_usage) => write!(f, "storage_usage: {}", storage_usage)?,
                };

                write!(f, ")")
            }
            Feature::ClusterQuota(v) => {
                write!(f, "cluster_quota(")?;

                match &v.max_clusters {
                    None => write!(f, "max_clusters: unlimited,")?,
                    Some(v) => write!(f, "max_clusters: {}", v)?,
                };

                match v.max_nodes_per_cluster {
                    None => write!(f, "max_nodes_per_cluster: unlimited,")?,
                    Some(v) => write!(f, "max_nodes_per_cluster: {}", v)?,
                };

                write!(f, ")")
            }
            Feature::AmendTable => write!(f, "amend_table"),
            Feature::Unknown => write!(f, "unknown"),
        }
    }
}

impl Feature {
    pub fn verify_default(&self, message: impl Into<String>) -> Result<(), ErrorCode> {
        match self {
            Feature::ClusterQuota(cluster_quote) => {
                if matches!(cluster_quote.max_clusters, Some(x) if x > 1) {
                    return Err(ErrorCode::LicenseKeyInvalid(
                        "No license found. The default configuration of Databend Community Edition only supports 1 cluster. To use more clusters, please consider upgrading to Databend Enterprise Edition. Learn more at https://docs.databend.com/guides/overview/editions/dee/",
                    ));
                }

                if matches!(cluster_quote.max_nodes_per_cluster, Some(x) if x > 1) {
                    return Err(ErrorCode::LicenseKeyInvalid(
                        "No license found. The default configuration of Databend Community Edition only supports up to 1 nodes per cluster. To use more nodes per cluster, please consider upgrading to Databend Enterprise Edition. Learn more at https://docs.databend.com/guides/overview/editions/dee/",
                    ));
                }

                Ok(())
            }
            _ => Err(ErrorCode::LicenseKeyInvalid(message.into())),
        }
    }

    pub fn verify(&self, feature: &Feature) -> Result<bool, ErrorCode> {
        match (self, feature) {
            (Feature::ComputeQuota(c), Feature::ComputeQuota(v)) => {
                if let Some(thread_num) = c.threads_num {
                    if thread_num <= v.threads_num.unwrap_or(usize::MAX) {
                        return Ok(false);
                    }
                }

                if let Some(max_memory_usage) = c.memory_usage {
                    if max_memory_usage <= v.memory_usage.unwrap_or(usize::MAX) {
                        return Ok(false);
                    }
                }

                Ok(true)
            }
            (Feature::StorageQuota(c), Feature::StorageQuota(v)) => {
                if let Some(max_storage_usage) = c.storage_usage {
                    if max_storage_usage <= v.storage_usage.unwrap_or(usize::MAX) {
                        return Ok(false);
                    }
                }

                Ok(true)
            }
            (Feature::ClusterQuota(c), Feature::ClusterQuota(v)) => {
                if let Some(max_clusters) = c.max_clusters {
                    if max_clusters < v.max_clusters.unwrap_or(usize::MAX) {
                        return Err(ErrorCode::LicenseKeyInvalid(format!(
                            "The number of clusters exceeds the quota specified in the Databend Enterprise Edition license. Maximum allowed: {}, Requested: {}. Please contact Databend to review your licensing options. Learn more at https://docs.databend.com/guides/overview/editions/dee/",
                            max_clusters,
                            v.max_clusters.unwrap_or(usize::MAX)
                        )));
                    }
                }

                if let Some(max_nodes_per_cluster) = c.max_nodes_per_cluster {
                    if max_nodes_per_cluster < v.max_nodes_per_cluster.unwrap_or(usize::MAX) {
                        return Err(ErrorCode::LicenseKeyInvalid(format!(
                            "The number of nodes per cluster exceeds the quota specified in the Databend Enterprise Edition license. Maximum allowed: {}, Requested: {}. Please contact Databend to review your licensing options. Learn more at https://docs.databend.com/guides/overview/editions/dee/",
                            max_nodes_per_cluster,
                            v.max_nodes_per_cluster.unwrap_or(usize::MAX)
                        )));
                    }
                }

                Ok(true)
            }
            (Feature::Test, Feature::Test)
            | (Feature::AggregateIndex, Feature::AggregateIndex)
            | (Feature::ComputedColumn, Feature::ComputedColumn)
            | (Feature::Vacuum, Feature::Vacuum)
            | (Feature::LicenseInfo, Feature::LicenseInfo)
            | (Feature::Stream, Feature::Stream)
            | (Feature::BackgroundService, Feature::BackgroundService)
            | (Feature::DataMask, Feature::DataMask)
            | (Feature::InvertedIndex, Feature::InvertedIndex)
            | (Feature::VirtualColumn, Feature::VirtualColumn)
            | (Feature::AttacheTable, Feature::AttacheTable)
            | (Feature::StorageEncryption, Feature::StorageEncryption) => Ok(true),
            (_, _) => Ok(false),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct LicenseInfo {
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub org: Option<String>,
    pub tenants: Option<Vec<String>>,
    pub features: Option<Vec<Feature>>,
}

impl LicenseInfo {
    pub fn display_features(&self) -> String {
        // sort all features in alphabet order and ignore test feature
        let mut features = self.features.clone().unwrap_or_default();

        if features.is_empty() {
            return String::from("Unlimited");
        }

        features.sort();

        features
            .iter()
            .filter(|f| **f != Feature::Test)
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Get Storage Quota from given license info.
    ///
    /// Returns the default storage quota if the storage quota is not licensed.
    pub fn get_storage_quota(&self) -> StorageQuota {
        let Some(features) = self.features.as_ref() else {
            return StorageQuota::default();
        };

        features
            .iter()
            .find_map(|f| match f {
                Feature::StorageQuota(v) => Some(v),
                _ => None,
            })
            .cloned()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_feature_from_string() {
        assert_eq!(
            Feature::LicenseInfo,
            serde_json::from_str::<Feature>("\"license_info\"").unwrap()
        );
        assert_eq!(
            Feature::Vacuum,
            serde_json::from_str::<Feature>("\"Vacuum\"").unwrap()
        );
        assert_eq!(
            Feature::Test,
            serde_json::from_str::<Feature>("\"Test\"").unwrap()
        );
        assert_eq!(
            Feature::VirtualColumn,
            serde_json::from_str::<Feature>("\"VIRTUAL_COLUMN\"").unwrap()
        );
        assert_eq!(
            Feature::BackgroundService,
            serde_json::from_str::<Feature>("\"BackgroundService\"").unwrap()
        );
        assert_eq!(
            Feature::DataMask,
            serde_json::from_str::<Feature>("\"DataMask\"").unwrap()
        );
        assert_eq!(
            Feature::AggregateIndex,
            serde_json::from_str::<Feature>("\"AggregateIndex\"").unwrap()
        );
        assert_eq!(
            Feature::InvertedIndex,
            serde_json::from_str::<Feature>("\"InvertedIndex\"").unwrap()
        );
        assert_eq!(
            Feature::ComputedColumn,
            serde_json::from_str::<Feature>("\"ComputedColumn\"").unwrap()
        );
        assert_eq!(
            Feature::StorageEncryption,
            serde_json::from_str::<Feature>("\"StorageEncryption\"").unwrap()
        );
        assert_eq!(
            Feature::Stream,
            serde_json::from_str::<Feature>("\"Stream\"").unwrap()
        );
        assert_eq!(
            Feature::AttacheTable,
            serde_json::from_str::<Feature>("\"ATTACH_TABLE\"").unwrap()
        );
        assert_eq!(
            Feature::ComputeQuota(ComputeQuota {
                threads_num: Some(1),
                memory_usage: Some(1),
            }),
            serde_json::from_str::<Feature>(
                "{\"ComputeQuota\":{\"threads_num\":1, \"memory_usage\":1}}"
            )
            .unwrap()
        );

        assert_eq!(
            Feature::ComputeQuota(ComputeQuota {
                threads_num: None,
                memory_usage: Some(1),
            }),
            serde_json::from_str::<Feature>("{\"ComputeQuota\":{\"memory_usage\":1}}").unwrap()
        );

        assert_eq!(
            Feature::StorageQuota(StorageQuota {
                storage_usage: Some(1),
            }),
            serde_json::from_str::<Feature>("{\"StorageQuota\":{\"storage_usage\":1}}").unwrap()
        );

        assert_eq!(
            Feature::ClusterQuota(ClusterQuota {
                max_clusters: None,
                max_nodes_per_cluster: Some(1),
            }),
            serde_json::from_str::<Feature>("{\"ClusterQuota\":{\"max_nodes_per_cluster\":1}}")
                .unwrap()
        );

        assert_eq!(
            Feature::AmendTable,
            serde_json::from_str::<Feature>("\"amend_table\"").unwrap()
        );

        assert_eq!(
            Feature::Unknown,
            serde_json::from_str::<Feature>("\"ssss\"").unwrap()
        );
    }

    #[test]
    fn test_cluster_quota_verify_default() {
        assert!(
            Feature::ClusterQuota(ClusterQuota::limit_clusters(1))
                .verify_default("")
                .is_ok()
        );
        assert!(
            Feature::ClusterQuota(ClusterQuota::limit_nodes(1))
                .verify_default("")
                .is_ok()
        );
        assert!(
            Feature::ClusterQuota(ClusterQuota::limit_nodes(2))
                .verify_default("")
                .is_err()
        );

        for nodes in 0..2 {
            assert!(
                Feature::ClusterQuota(ClusterQuota::limit_full(1, nodes))
                    .verify_default("")
                    .is_ok()
            );
        }

        assert!(
            Feature::ClusterQuota(ClusterQuota::limit_clusters(2))
                .verify_default("")
                .is_err()
        );
        assert!(
            Feature::ClusterQuota(ClusterQuota::limit_nodes(4))
                .verify_default("")
                .is_err()
        );
        assert!(
            Feature::ClusterQuota(ClusterQuota::limit_full(2, 1))
                .verify_default("")
                .is_err()
        );
        assert!(
            Feature::ClusterQuota(ClusterQuota::limit_full(1, 4))
                .verify_default("")
                .is_err()
        );
    }

    #[test]
    fn test_cluster_quota_verify() -> Result<(), ErrorCode> {
        let unlimit_feature = Feature::ClusterQuota(ClusterQuota::un_limit());

        for cluster_num in 0..1000 {
            for node_num in 0..1000 {
                let feature =
                    Feature::ClusterQuota(ClusterQuota::limit_full(cluster_num, node_num));
                assert!(unlimit_feature.verify(&feature)?);
            }
        }

        let unlimit_cluster_feature = Feature::ClusterQuota(ClusterQuota::limit_nodes(1));

        for cluster_num in 0..1000 {
            let feature = Feature::ClusterQuota(ClusterQuota::limit_full(cluster_num, 1));
            assert!(unlimit_cluster_feature.verify(&feature)?);
            let feature = Feature::ClusterQuota(ClusterQuota::limit_full(cluster_num, 2));
            assert!(unlimit_cluster_feature.verify(&feature).is_err());
        }

        let unlimit_nodes_feature = Feature::ClusterQuota(ClusterQuota::limit_clusters(1));

        for nodes_num in 0..1000 {
            let feature = Feature::ClusterQuota(ClusterQuota::limit_full(1, nodes_num));
            assert!(unlimit_nodes_feature.verify(&feature)?);
            let feature = Feature::ClusterQuota(ClusterQuota::limit_full(2, nodes_num));
            assert!(unlimit_nodes_feature.verify(&feature).is_err());
        }

        let limit_full = Feature::ClusterQuota(ClusterQuota::limit_full(1, 1));
        let feature = Feature::ClusterQuota(ClusterQuota::limit_full(1, 1));
        assert!(limit_full.verify(&feature)?);
        let feature = Feature::ClusterQuota(ClusterQuota::limit_full(2, 1));
        assert!(limit_full.verify(&feature).is_err());
        let feature = Feature::ClusterQuota(ClusterQuota::limit_full(1, 2));
        assert!(limit_full.verify(&feature).is_err());

        Ok(())
    }
}
