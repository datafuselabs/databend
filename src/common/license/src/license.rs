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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ComputeQuota {
    threads_num: Option<usize>,
    memory_usage: Option<usize>,
}

// All enterprise features are defined here.
#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Feature {
    LicenseInfo,
    Vacuum,
    Test,
    VirtualColumn,
    BackgroundService,
    DataMask,
    AggregateIndex,
    InvertedIndex,
    ComputedColumn,
    StorageEncryption,
    Stream,
    ComputeQuota(ComputeQuota),
}

impl Display for Feature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
            Feature::ComputeQuota(v) => {
                write!(f, "compute_quota(")?;

                match &v.threads_num {
                    None => write!(f, "threads_num: unlimited,")?,
                    Some(threads_num) => write!(f, "threads_num: {}", *threads_num)?,
                };

                match v.memory_usage {
                    None => write!(f, "memory_usage: unlimited,"),
                    Some(memory_usage) => write!(f, "memory_usage: {}", memory_usage),
                }
            }
        }
    }
}

impl Feature {
    pub fn verify(&self, feature: &Feature) -> bool {
        self == feature
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
}

#[cfg(test)]
mod tests {
    use crate::license::ComputeQuota;
    use crate::license::Feature;

    #[test]
    fn test_deserialize_feature_from_string() {
        assert_eq!(
            Feature::LicenseInfo,
            serde_json::from_str::<Feature>("\"LicenseInfo\"").unwrap()
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
            serde_json::from_str::<Feature>("\"VirtualColumn\"").unwrap()
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
            Feature::ComputeQuota(ComputeQuota {
                threads_num: Some(1),
                memory_usage: Some(1),
            }),
            serde_json::from_str::<Feature>(
                "{\"ComputeQuota\":{\"threads_num\":1, \"memory_usage\":1}}"
            )
            .unwrap()
        )
    }
}
