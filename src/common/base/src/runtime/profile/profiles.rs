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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;

use once_cell::sync::OnceCell;

use crate::base::convert_byte_size;
use crate::base::convert_number_size;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize, Debug)]
pub enum ProfileStatisticsName {
    /// The time spent to process in nanoseconds
    CpuTime,
    /// The time spent to wait in nanoseconds, usually used to
    /// measure the time spent on waiting for I/O
    WaitTime,
    ExchangeRows,
    ExchangeBytes,
    OutputRows,
    OutputBytes,
    ScanBytes,
    ScanCacheBytes,
    ScanPartitions,
    SpillWriteCount,
    SpillWriteBytes,
    SpillWriteTime,
    SpillReadCount,
    SpillReadBytes,
    SpillReadTime,
    RuntimeFilterPruneParts,
    RuntimeFilterMergeIntoSourceBuildBloomPruneParts,
    MemoryUsage,
}

#[derive(Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize, Debug)]
pub enum StatisticsUnit {
    Rows,
    Bytes,
    NanoSeconds,
    MillisSeconds,
    Count,
}

impl Display for ProfileStatisticsName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<usize> for ProfileStatisticsName {
    fn from(value: usize) -> Self {
        let statistics_index = get_statistics_name_index();

        if value > statistics_index.len() {
            panic!("logical error");
        }

        match &statistics_index[value] {
            None => panic!("logical error {}", value),
            Some(statistics_name) => statistics_name.clone(),
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileDesc {
    pub desc: &'static str,
    pub display_name: &'static str,
    pub index: usize,
    pub unit: StatisticsUnit,
    pub plain_statistics: bool,
}

impl ProfileDesc {
    pub fn human_format(&self, value: usize) -> String {
        match self.unit {
            StatisticsUnit::Rows => convert_number_size(value as f64),
            StatisticsUnit::Bytes => convert_byte_size(value as f64),
            StatisticsUnit::NanoSeconds => format!("{:?}", Duration::from_nanos(value as u64)),
            StatisticsUnit::MillisSeconds => format!("{:?}", Duration::from_millis(value as u64)),
            StatisticsUnit::Count => format!("{}", value),
        }
    }
}

pub static PROFILES_DESC: OnceCell<Arc<BTreeMap<ProfileStatisticsName, ProfileDesc>>> =
    OnceCell::new();

pub static PROFILES_INDEX: OnceCell<
    Arc<[Option<ProfileStatisticsName>; std::mem::variant_count::<ProfileStatisticsName>()]>,
> = OnceCell::new();

pub fn get_statistics_name_index()
-> Arc<[Option<ProfileStatisticsName>; std::mem::variant_count::<ProfileStatisticsName>()]> {
    PROFILES_INDEX
        .get_or_init(|| {
            let statistics_desc = get_statistics_desc();
            let mut statistics_index = std::array::from_fn(|_v| None);

            for (k, v) in statistics_desc.iter() {
                statistics_index[v.index] = Some(k.clone());
            }

            Arc::new(statistics_index)
        })
        .clone()
}

pub fn get_statistics_desc() -> Arc<BTreeMap<ProfileStatisticsName, ProfileDesc>> {
    PROFILES_DESC.get_or_init(|| {
        Arc::new(BTreeMap::from([
            (ProfileStatisticsName::CpuTime, ProfileDesc {
                display_name: "cpu time",
                desc: "The time spent to process in nanoseconds",
                index: ProfileStatisticsName::CpuTime as usize,
                unit: StatisticsUnit::NanoSeconds,
                plain_statistics: false,
            }),
            (ProfileStatisticsName::WaitTime, ProfileDesc {
                display_name: "wait time",
                desc: "The time spent to wait in nanoseconds, usually used to measure the time spent on waiting for I/O",
                index: ProfileStatisticsName::WaitTime as usize,
                unit: StatisticsUnit::NanoSeconds,
                plain_statistics: false,
            }),
            (ProfileStatisticsName::ExchangeRows, ProfileDesc {
                display_name: "exchange rows",
                desc: "The number of data rows exchange between nodes in cluster mode",
                index: ProfileStatisticsName::ExchangeRows as usize,
                unit: StatisticsUnit::Rows,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::ExchangeBytes, ProfileDesc {
                display_name: "exchange bytes",
                desc: "The number of data bytes exchange between nodes in cluster mode",
                index: ProfileStatisticsName::ExchangeBytes as usize,
                unit: StatisticsUnit::Bytes,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::OutputRows, ProfileDesc {
                display_name: "output rows",
                desc: "The number of rows from the physical plan output to the next physical plan",
                index: ProfileStatisticsName::OutputRows as usize,
                unit: StatisticsUnit::Rows,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::OutputBytes, ProfileDesc {
                display_name: "output bytes",
                desc: "The number of bytes from the physical plan output to the next physical plan",
                index: ProfileStatisticsName::OutputBytes as usize,
                unit: StatisticsUnit::Bytes,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::ScanBytes, ProfileDesc {
                display_name: "bytes scanned",
                desc: "The bytes scanned of query",
                index: ProfileStatisticsName::ScanBytes as usize,
                unit: StatisticsUnit::Bytes,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::ScanCacheBytes, ProfileDesc {
                display_name: "bytes scanned from cache",
                desc: "The bytes scanned from cache of query",
                index: ProfileStatisticsName::ScanCacheBytes as usize,
                unit: StatisticsUnit::Bytes,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::ScanPartitions, ProfileDesc {
                display_name: "partitions scanned",
                desc: "The partitions scanned of query",
                index: ProfileStatisticsName::ScanPartitions as usize,
                unit: StatisticsUnit::Count,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::SpillWriteCount, ProfileDesc {
                display_name: "numbers spilled by write",
                desc: "The number of spilled by write",
                index: ProfileStatisticsName::SpillWriteCount as usize,
                unit: StatisticsUnit::Count,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::SpillWriteBytes, ProfileDesc {
                display_name: "bytes spilled by write",
                desc: "The bytes spilled by write",
                index: ProfileStatisticsName::SpillWriteBytes as usize,
                unit: StatisticsUnit::Bytes,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::SpillWriteTime, ProfileDesc {
                display_name: "spilled time by write",
                desc: "The time spent to write spill in millisecond",
                index: ProfileStatisticsName::SpillWriteTime as usize,
                unit: StatisticsUnit::MillisSeconds,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::SpillReadCount, ProfileDesc {
                display_name: "numbers spilled by read",
                desc: "The number of spilled by read",
                index: ProfileStatisticsName::SpillReadCount as usize,
                unit: StatisticsUnit::Count,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::SpillReadBytes, ProfileDesc {
                display_name: "bytes spilled by read",
                desc: "The bytes spilled by read",
                index: ProfileStatisticsName::SpillReadBytes as usize,
                unit: StatisticsUnit::Bytes,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::SpillReadTime, ProfileDesc {
                display_name: "spilled time by read",
                desc: "The time spent to read spill in millisecond",
                index: ProfileStatisticsName::SpillReadTime as usize,
                unit: StatisticsUnit::MillisSeconds,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::RuntimeFilterPruneParts, ProfileDesc {
                display_name: "parts pruned by runtime filter",
                desc: "The partitions pruned by runtime filter",
                index: ProfileStatisticsName::RuntimeFilterPruneParts as usize,
                unit: StatisticsUnit::Count,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::RuntimeFilterMergeIntoSourceBuildBloomPruneParts, ProfileDesc {
                display_name: "parts pruned by merge into target table bloom filter",
                desc: "The partitions pruned by merge into target table bloom filter",
                index: ProfileStatisticsName::RuntimeFilterMergeIntoSourceBuildBloomPruneParts as usize,
                unit: StatisticsUnit::Count,
                plain_statistics: true,
            }),
            (ProfileStatisticsName::MemoryUsage, ProfileDesc {
                display_name: "memory usage",
                desc: "The real time memory usage",
                index: ProfileStatisticsName::MemoryUsage as usize,
                unit: StatisticsUnit::Bytes,
                plain_statistics: false,
            })
        ]))
    }).clone()
}
