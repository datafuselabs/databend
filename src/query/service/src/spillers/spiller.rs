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

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Range;
use std::ptr::Alignment;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::dma_buffer_as_vec;
use databend_common_base::base::dma_read_file_range;
use databend_common_base::base::DmaWriteBuf;
use databend_common_base::base::GlobalUniqName;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column_in;
use databend_common_expression::DataBlock;
use databend_storages_common_cache::TempDir;
use databend_storages_common_cache::TempPath;
use opendal::Operator;

use crate::sessions::QueryContext;

/// Spiller type, currently only supports HashJoin
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpillerType {
    HashJoinBuild,
    HashJoinProbe,
    Window,
    OrderBy,
    // Todo: Add more spillers type
    // Aggregation
}

impl Display for SpillerType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SpillerType::HashJoinBuild => write!(f, "HashJoinBuild"),
            SpillerType::HashJoinProbe => write!(f, "HashJoinProbe"),
            SpillerType::Window => write!(f, "Window"),
            SpillerType::OrderBy => write!(f, "OrderBy"),
        }
    }
}

/// Spiller configuration
#[derive(Clone)]
pub struct SpillerConfig {
    pub location_prefix: String,
    pub disk_spill: Option<Arc<TempDir>>,
    pub spiller_type: SpillerType,
}

/// Spiller is a unified framework for operators which need to spill data from memory.
/// It provides the following features:
/// 1. Collection data that needs to be spilled.
/// 2. Partition data by the specified algorithm which specifies by operator
/// 3. Serialization and deserialization input data
/// 4. Interact with the underlying storage engine to write and read spilled data
#[derive(Clone)]
pub struct Spiller {
    ctx: Arc<QueryContext>,
    operator: Operator,
    location_prefix: String,
    disk_spill: Option<Arc<TempDir>>,
    _spiller_type: SpillerType,
    pub join_spilling_partition_bits: usize,
    /// 1 partition -> N partition files
    pub partition_location: HashMap<usize, Vec<Location>>,
    /// Record columns layout for spilled data, will be used when read data from disk
    pub columns_layout: HashMap<Location, Vec<usize>>,
    /// Record how many bytes have been spilled for each partition.
    pub partition_spilled_bytes: HashMap<usize, u64>,
}

impl Spiller {
    /// Create a new spiller
    pub fn create(
        ctx: Arc<QueryContext>,
        operator: Operator,
        config: SpillerConfig,
    ) -> Result<Self> {
        let join_spilling_partition_bits = ctx.get_settings().get_join_spilling_partition_bits()?;
        let SpillerConfig {
            location_prefix,
            disk_spill,
            spiller_type,
        } = config;
        Ok(Self {
            ctx,
            operator,
            location_prefix,
            disk_spill,
            _spiller_type: spiller_type,
            join_spilling_partition_bits,
            partition_location: Default::default(),
            columns_layout: Default::default(),
            partition_spilled_bytes: Default::default(),
        })
    }

    pub fn spilled_partitions(&self) -> HashSet<usize> {
        self.partition_location.keys().copied().collect()
    }

    /// Spill a [`DataBlock`] to storage.
    pub async fn spill(&mut self, data_block: DataBlock) -> Result<Location> {
        let instant = Instant::now();

        // Spill data to storage.
        let mut encoder = self.block_encoder();
        encoder.add_block(data_block);
        let data_size = encoder.size();
        let BlocksEncoder {
            buf,
            mut columns_layout,
            ..
        } = encoder;

        let location = self.write_encodes(data_size, buf).await?;

        // Record statistics.
        match location {
            Location::Remote(_) => record_remote_write_profile(&instant, data_size),
            Location::Local(_) => record_local_write_profile(&instant, data_size),
        }

        // Record columns layout for spilled data.
        self.columns_layout
            .insert(location.clone(), columns_layout.pop().unwrap());

        Ok(location)
    }

    #[async_backtrace::framed]
    /// Spill data block with partition
    pub async fn spill_with_partition(
        &mut self,
        partition_id: usize,
        data: DataBlock,
    ) -> Result<()> {
        let progress_val = ProgressValues {
            rows: data.num_rows(),
            bytes: data.memory_size(),
        };

        self.partition_spilled_bytes
            .entry(partition_id)
            .and_modify(|bytes| {
                *bytes += data.memory_size() as u64;
            })
            .or_insert(data.memory_size() as u64);

        let location = self.spill(data).await?;
        self.partition_location
            .entry(partition_id)
            .and_modify(|locs| {
                locs.push(location.clone());
            })
            .or_insert(vec![location.clone()]);

        self.ctx.get_join_spill_progress().incr(&progress_val);
        Ok(())
    }

    pub async fn spill_with_merged_partitions(
        &mut self,
        partitioned_data: Vec<(usize, DataBlock)>,
    ) -> Result<SpilledData> {
        // Serialize data block.
        let mut encoder = self.block_encoder();
        let mut partition_ids = Vec::new();
        for (partition_id, data_block) in partitioned_data.into_iter() {
            partition_ids.push(partition_id);
            encoder.add_block(data_block);
        }

        let write_bytes = encoder.size();
        let BlocksEncoder {
            buf,
            offsets,
            columns_layout,
            ..
        } = encoder;

        let partitions = partition_ids
            .into_iter()
            .zip(
                offsets
                    .windows(2)
                    .map(|x| x[0]..x[1])
                    .zip(columns_layout.into_iter()),
            )
            .map(|(id, (range, layout))| (id, range, layout))
            .collect();

        // Spill data to storage.
        let instant = Instant::now();
        let location = self.write_encodes(write_bytes, buf).await?;

        // Record statistics.
        match location {
            Location::Remote(_) => record_remote_write_profile(&instant, write_bytes),
            Location::Local(_) => record_local_write_profile(&instant, write_bytes),
        }

        Ok(SpilledData::MergedPartition {
            location,
            partitions,
        })
    }

    /// Read a certain file to a [`DataBlock`].
    /// We should guarantee that the file is managed by this spiller.
    pub async fn read_spilled_file(&self, location: &Location) -> Result<DataBlock> {
        let columns_layout = self.columns_layout.get(location).unwrap();

        // Read spilled data from storage.
        let instant = Instant::now();
        let block = match location {
            Location::Remote(loc) => {
                let data = self.operator.read(loc).await?.to_bytes();
                record_remote_read_profile(&instant, data.len());
                deserialize_block(columns_layout, &data)
            }
            Location::Local(path) => {
                let file_size = path.size();
                debug_assert_eq!(file_size, columns_layout.iter().sum::<usize>());
                let (buf, range) = dma_read_file_range(path, 0..file_size as u64).await?;
                let data = &buf[range];
                record_local_read_profile(&instant, data.len());
                deserialize_block(columns_layout, data)
            }
        };

        Ok(block)
    }

    #[async_backtrace::framed]
    /// Read spilled data with partition id
    pub async fn read_spilled_partition(&mut self, p_id: &usize) -> Result<Vec<DataBlock>> {
        if let Some(locs) = self.partition_location.get(p_id) {
            let mut spilled_data = Vec::with_capacity(locs.len());
            for loc in locs.iter() {
                let block = self.read_spilled_file(loc).await?;

                if block.num_rows() != 0 {
                    spilled_data.push(block);
                }
            }
            Ok(spilled_data)
        } else {
            Ok(vec![])
        }
    }

    pub async fn read_merged_partitions(
        &self,
        merged_partitions: &SpilledData,
    ) -> Result<Vec<(usize, DataBlock)>> {
        if let SpilledData::MergedPartition {
            location,
            partitions,
        } = merged_partitions
        {
            // Read spilled data from storage.
            let instant = Instant::now();

            let data = match location {
                Location::Remote(loc) => self.operator.read(loc).await?.to_bytes(),
                Location::Local(path) => {
                    let file_size = path.size();
                    debug_assert_eq!(
                        file_size,
                        if let Some((_, range, _)) = partitions.last() {
                            range.end
                        } else {
                            0
                        }
                    );

                    let (mut buf, range) = dma_read_file_range(path, 0..file_size as u64).await?;
                    assert_eq!(range.start, 0);
                    buf.truncate(range.end);

                    dma_buffer_as_vec(buf).into()
                }
            };

            // Record statistics.
            match location {
                Location::Remote(_) => record_remote_read_profile(&instant, data.len()),
                Location::Local(_) => record_local_read_profile(&instant, data.len()),
            };

            // Deserialize partitioned data block.
            let partitioned_data = partitions
                .iter()
                .map(|(partition_id, range, columns_layout)| {
                    let block = deserialize_block(columns_layout, &data[range.clone()]);
                    (*partition_id, block)
                })
                .collect();

            return Ok(partitioned_data);
        }
        Ok(vec![])
    }

    pub async fn read_range(
        &self,
        location: &Location,
        data_range: Range<usize>,
        columns_layout: &[usize],
    ) -> Result<DataBlock> {
        // Read spilled data from storage.
        let instant = Instant::now();
        let data_range = data_range.start as u64..data_range.end as u64;

        match location {
            Location::Remote(loc) => {
                let data = self
                    .operator
                    .read_with(loc)
                    .range(data_range)
                    .await?
                    .to_bytes();
                record_remote_read_profile(&instant, data.len());
                Ok(deserialize_block(columns_layout, &data))
            }
            Location::Local(path) => {
                let (buf, range) = dma_read_file_range(path, data_range).await?;
                let data = &buf[range];
                record_local_read_profile(&instant, data.len());
                Ok(deserialize_block(columns_layout, data))
            }
        }
    }

    async fn write_encodes(&mut self, size: usize, buf: DmaWriteBuf) -> Result<Location> {
        let location = match &self.disk_spill {
            None => None,
            Some(disk) => disk.new_file_with_size(size)?.map(Location::Local),
        }
        .unwrap_or(Location::Remote(format!(
            "{}/{}",
            self.location_prefix,
            GlobalUniqName::unique(),
        )));

        let written = match &location {
            Location::Remote(loc) => {
                let mut writer = self
                    .operator
                    .writer_with(loc)
                    .chunk(8 * 1024 * 1024)
                    .await?;

                let mut written = 0;
                for data in buf.into_data() {
                    written += data.len();
                    writer.write(dma_buffer_as_vec(data)).await?;
                }

                writer.close().await?;
                written
            }
            Location::Local(path) => buf.into_file(path).await?,
        };
        debug_assert_eq!(size, written);
        Ok(location)
    }

    fn block_encoder(&self) -> BlocksEncoder {
        let align = self
            .disk_spill
            .as_ref()
            .map(|dir| dir.block_alignment())
            .unwrap_or(Alignment::MIN);
        BlocksEncoder::new(align, 8 * 1024 * 1024)
    }

    pub(crate) fn spilled_files(&self) -> Vec<Location> {
        self.columns_layout.keys().cloned().collect()
    }
}

pub enum SpilledData {
    Partition(Location),
    MergedPartition {
        location: Location,
        partitions: Vec<(usize, Range<usize>, Vec<usize>)>,
    },
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Location {
    Remote(String),
    Local(TempPath),
}

struct BlocksEncoder {
    buf: DmaWriteBuf,
    offsets: Vec<usize>,
    columns_layout: Vec<Vec<usize>>,
}

impl BlocksEncoder {
    fn new(align: Alignment, chunk: usize) -> Self {
        Self {
            buf: DmaWriteBuf::new(align, chunk),
            offsets: vec![0],
            columns_layout: Vec::new(),
        }
    }

    fn add_block(&mut self, block: DataBlock) {
        let columns_layout = std::iter::once(self.size())
            .chain(block.columns().iter().map(|entry| {
                let column = entry
                    .value
                    .convert_to_full_column(&entry.data_type, block.num_rows());
                serialize_column_in(&column, &mut self.buf).unwrap();
                self.size()
            }))
            .map_windows(|x: &[_; 2]| x[1] - x[0])
            .collect();

        self.columns_layout.push(columns_layout);
        self.offsets.push(self.size())
    }

    fn size(&self) -> usize {
        self.buf.size()
    }
}

pub fn deserialize_block(columns_layout: &[usize], mut data: &[u8]) -> DataBlock {
    let columns = columns_layout
        .iter()
        .map(|layout| {
            let (cur, remain) = data.split_at(*layout);
            data = remain;
            deserialize_column(cur).unwrap()
        })
        .collect::<Vec<_>>();

    DataBlock::new_from_columns(columns)
}

pub fn record_remote_write_profile(start: &Instant, write_bytes: usize) {
    Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteCount, 1);
    Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteBytes, write_bytes);
    Profile::record_usize_profile(
        ProfileStatisticsName::RemoteSpillWriteTime,
        start.elapsed().as_millis() as usize,
    );
}

pub fn record_remote_read_profile(start: &Instant, read_bytes: usize) {
    Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadCount, 1);
    Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadBytes, read_bytes);
    Profile::record_usize_profile(
        ProfileStatisticsName::RemoteSpillReadTime,
        start.elapsed().as_millis() as usize,
    );
}

pub fn record_local_write_profile(start: &Instant, write_bytes: usize) {
    Profile::record_usize_profile(ProfileStatisticsName::LocalSpillWriteCount, 1);
    Profile::record_usize_profile(ProfileStatisticsName::LocalSpillWriteBytes, write_bytes);
    Profile::record_usize_profile(
        ProfileStatisticsName::LocalSpillWriteTime,
        start.elapsed().as_millis() as usize,
    );
}

pub fn record_local_read_profile(start: &Instant, read_bytes: usize) {
    Profile::record_usize_profile(ProfileStatisticsName::LocalSpillReadCount, 1);
    Profile::record_usize_profile(ProfileStatisticsName::LocalSpillReadBytes, read_bytes);
    Profile::record_usize_profile(
        ProfileStatisticsName::LocalSpillReadTime,
        start.elapsed().as_millis() as usize,
    );
}
