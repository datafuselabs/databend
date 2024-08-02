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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::vec;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use databend_common_arrow::arrow_format::flight::data::FlightData;
use databend_common_catalog::statistics::data_cache_statistics::DataCacheMetricValues;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::PlanProfile;
use databend_common_storage::CopyStatus;
use databend_common_storage::MergeStatus;
use log::error;

use crate::servers::flight::v1::packets::ProgressInfo;

pub struct FragmentData {
    meta: Vec<u8>,
    pub data: FlightData,
}

impl FragmentData {
    pub fn get_meta(&self) -> &[u8] {
        &self.meta[0..self.meta.len() - 1]
    }

    pub fn create(meta: Vec<u8>, data: FlightData) -> FragmentData {
        FragmentData { meta, data }
    }
}

impl Debug for FragmentData {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("FragmentData").finish()
    }
}

pub enum DataPacket {
    ErrorCode(ErrorCode),
    Dictionary(FlightData),
    FragmentData(FragmentData),
    QueryProfiles(HashMap<u32, PlanProfile>),
    SerializeProgress(Vec<ProgressInfo>),
    CopyStatus(CopyStatus),
    MergeStatus(MergeStatus),
    DataCacheMetrics(DataCacheMetricValues),
}

fn calc_size(flight_data: &FlightData) -> usize {
    flight_data.data_body.len() + flight_data.data_header.len() + flight_data.app_metadata.len()
}

impl DataPacket {
    pub fn bytes_size(&self) -> usize {
        match self {
            DataPacket::ErrorCode(_) => 0,
            DataPacket::CopyStatus(_) => 0,
            DataPacket::MergeStatus(_) => 0,
            DataPacket::SerializeProgress(_) => 0,
            DataPacket::Dictionary(v) => calc_size(v),
            DataPacket::FragmentData(v) => calc_size(&v.data) + v.meta.len(),
            DataPacket::QueryProfiles(_) => 0,
            DataPacket::DataCacheMetrics(_) => 0,
        }
    }
}

impl TryFrom<DataPacket> for FlightData {
    type Error = ErrorCode;

    fn try_from(packet: DataPacket) -> Result<Self> {
        Ok(match packet {
            DataPacket::ErrorCode(error) => {
                error!("Got error code data packet: {:?}", error);
                FlightData::from(error)
            }
            DataPacket::FragmentData(fragment_data) => FlightData::from(fragment_data),
            DataPacket::QueryProfiles(profiles) => FlightData {
                app_metadata: vec![0x03],
                data_body: serde_json::to_vec(&profiles)?,
                data_header: vec![],
                flight_descriptor: None,
            },
            DataPacket::SerializeProgress(progress) => {
                let mut data_body = vec![];
                data_body.write_u64::<BigEndian>(progress.len() as u64)?;

                // Progress.
                for progress_info in progress {
                    progress_info.write(&mut data_body)?;
                }

                FlightData {
                    data_body,
                    data_header: vec![],
                    flight_descriptor: None,
                    app_metadata: vec![0x04],
                }
            }
            DataPacket::Dictionary(mut flight_data) => {
                flight_data.app_metadata.push(0x05);
                flight_data
            }
            DataPacket::CopyStatus(status) => FlightData {
                app_metadata: vec![0x06],
                data_body: serde_json::to_vec(&status)?,
                data_header: vec![],
                flight_descriptor: None,
            },
            DataPacket::MergeStatus(status) => FlightData {
                app_metadata: vec![0x07],
                data_body: serde_json::to_vec(&status)?,
                data_header: vec![],
                flight_descriptor: None,
            },
            DataPacket::DataCacheMetrics(metrics) => FlightData {
                app_metadata: vec![0x08],
                data_body: serde_json::to_vec(&metrics)?,
                data_header: vec![],
                flight_descriptor: None,
            },
        })
    }
}

impl From<FragmentData> for FlightData {
    fn from(mut data: FragmentData) -> Self {
        data.meta.push(0x01);
        FlightData {
            app_metadata: data.meta,
            data_body: data.data.data_body,
            data_header: data.data.data_header,
            flight_descriptor: None,
        }
    }
}

impl TryFrom<FlightData> for DataPacket {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        if flight_data.app_metadata.is_empty() {
            return Err(ErrorCode::BadBytes("Flight data app metadata is empty."));
        }

        match flight_data.app_metadata.last().unwrap() {
            0x01 => Ok(DataPacket::FragmentData(FragmentData::try_from(
                flight_data,
            )?)),
            0x02 => Ok(DataPacket::ErrorCode(ErrorCode::try_from(flight_data)?)),
            0x03 => {
                let status =
                    serde_json::from_slice::<HashMap<u32, PlanProfile>>(&flight_data.data_body)?;
                Ok(DataPacket::QueryProfiles(status))
            }
            0x04 => {
                let mut bytes = flight_data.data_body.as_slice();
                let progress_size = bytes.read_u64::<BigEndian>()?;

                // Progress.
                let mut progress_info = Vec::with_capacity(progress_size as usize);
                for _index in 0..progress_size {
                    progress_info.push(ProgressInfo::read(&mut bytes)?);
                }

                Ok(DataPacket::SerializeProgress(progress_info))
            }
            0x05 => Ok(DataPacket::Dictionary(flight_data)),
            0x06 => {
                let status = serde_json::from_slice::<CopyStatus>(&flight_data.data_body)?;
                Ok(DataPacket::CopyStatus(status))
            }
            0x07 => {
                let status = serde_json::from_slice::<MergeStatus>(&flight_data.data_body)?;
                Ok(DataPacket::MergeStatus(status))
            }
            0x08 => {
                let status =
                    serde_json::from_slice::<DataCacheMetricValues>(&flight_data.data_body)?;
                Ok(DataPacket::DataCacheMetrics(status))
            }
            _ => Err(ErrorCode::BadBytes("Unknown flight data packet type.")),
        }
    }
}

impl TryFrom<FlightData> for FragmentData {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        Ok(FragmentData::create(flight_data.app_metadata, FlightData {
            app_metadata: vec![],
            flight_descriptor: None,
            data_body: flight_data.data_body,
            data_header: flight_data.data_header,
        }))
    }
}
