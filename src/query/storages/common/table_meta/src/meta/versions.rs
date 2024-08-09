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

use std::marker::PhantomData;

use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;

use super::v2;
use crate::meta::v0;
use crate::meta::v1;
use crate::meta::v3;
use crate::meta::v4;
use crate::meta::v5;

// Here versions of meta are tagged with numeric values
//
// The trait Versioned itself can not prevent us from
// giving multiple version numbers to a specific type, such as
//
// impl Versioned<0> for v0::SegmentInfo {}
// impl Versioned<1> for v0::SegmentInfo {}
//
// Fortunately, since v0::SegmentInfo::VERSION is used in
// several places, compiler will report compile error if it
// can not deduce a unique value the constant expression.

/// Thing has a u64 version number
pub trait Versioned<const V: u64>
where Self: Sized
{
    const VERSION: u64 = V;
}

impl Versioned<0> for v0::SegmentInfo {}
impl Versioned<1> for v1::SegmentInfo {}
impl Versioned<2> for v2::SegmentInfo {}
impl Versioned<3> for v3::SegmentInfo {}
impl Versioned<4> for v4::SegmentInfo {}

pub enum SegmentInfoVersion {
    V0(PhantomData<v0::SegmentInfo>),
    V1(PhantomData<v1::SegmentInfo>),
    V2(PhantomData<v2::SegmentInfo>),
    V3(PhantomData<v3::SegmentInfo>),
    V4(PhantomData<v4::SegmentInfo>),
}

impl SegmentInfoVersion {
    pub fn version(&self) -> u64 {
        match self {
            SegmentInfoVersion::V0(a) => Self::ver(a),
            SegmentInfoVersion::V1(a) => Self::ver(a),
            SegmentInfoVersion::V2(a) => Self::ver(a),
            SegmentInfoVersion::V3(a) => Self::ver(a),
            SegmentInfoVersion::V4(a) => Self::ver(a),
        }
    }

    fn ver<const V: u64, T: Versioned<V>>(_v: &PhantomData<T>) -> u64 {
        V
    }
}

impl Versioned<0> for v0::TableSnapshot {}
impl Versioned<1> for v1::TableSnapshot {}
impl Versioned<2> for v2::TableSnapshot {}
impl Versioned<3> for v3::TableSnapshot {}
impl Versioned<4> for v4::TableSnapshot {}
impl Versioned<5> for v5::TableSnapshot {}

pub enum SnapshotVersion {
    V0(PhantomData<v0::TableSnapshot>),
    V1(PhantomData<v1::TableSnapshot>),
    V2(PhantomData<v2::TableSnapshot>),
    V3(PhantomData<v3::TableSnapshot>),
    V4(PhantomData<v4::TableSnapshot>),
    V5(PhantomData<v5::TableSnapshot>),
}

impl SnapshotVersion {
    pub fn version(&self) -> u64 {
        match self {
            SnapshotVersion::V0(a) => Self::ver(a),
            SnapshotVersion::V1(a) => Self::ver(a),
            SnapshotVersion::V2(a) => Self::ver(a),
            SnapshotVersion::V3(a) => Self::ver(a),
            SnapshotVersion::V4(a) => Self::ver(a),
            SnapshotVersion::V5(a) => Self::ver(a),
        }
    }

    fn ver<const V: u64, T: Versioned<V>>(_v: &PhantomData<T>) -> u64 {
        V
    }
}

impl Versioned<0> for v1::TableSnapshotStatistics {}
impl Versioned<2> for v2::TableSnapshotStatistics {}
impl Versioned<3> for v3::TableSnapshotStatistics {}

impl Versioned<2> for DataBlock {}

pub enum TableSnapshotStatisticsVersion {
    V0(PhantomData<v1::TableSnapshotStatistics>),
    V2(PhantomData<v2::TableSnapshotStatistics>),
    V3(PhantomData<v3::TableSnapshotStatistics>),
}

impl TableSnapshotStatisticsVersion {
    pub fn version(&self) -> u64 {
        match self {
            TableSnapshotStatisticsVersion::V0(a) => Self::ver(a),
            TableSnapshotStatisticsVersion::V2(a) => Self::ver(a),
            TableSnapshotStatisticsVersion::V3(a) => Self::ver(a),
        }
    }

    fn ver<const V: u64, T: Versioned<V>>(_v: &PhantomData<T>) -> u64 {
        V
    }
}

/// Statically check that if T implements Versioned<U> where U equals V
#[inline]
pub fn testify_version<T, const V: u64>(t: PhantomData<T>) -> PhantomData<T>
where T: Versioned<V> {
    t
}

mod converters {

    use super::*;

    impl TryFrom<u64> for SegmentInfoVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(SegmentInfoVersion::V0(
                    // `ver_eq<_, 0>` is merely a static check to make sure that
                    // the type `v0::SegmentInfoVersion` do have a numeric version number of 0
                    testify_version::<_, 0>(PhantomData),
                )),
                1 => Ok(SegmentInfoVersion::V1(testify_version::<_, 1>(PhantomData))),
                2 => Ok(SegmentInfoVersion::V2(testify_version::<_, 2>(PhantomData))),
                3 => Ok(SegmentInfoVersion::V3(testify_version::<_, 3>(PhantomData))),
                4 => Ok(SegmentInfoVersion::V4(testify_version::<_, 4>(PhantomData))),
                _ => Err(ErrorCode::Internal(format!(
                    "unknown segment version {value}, versions supported: 0, 1, 2, 3, 4"
                ))),
            }
        }
    }

    impl TryFrom<u64> for SnapshotVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(SnapshotVersion::V0(testify_version::<_, 0>(PhantomData))),
                1 => Ok(SnapshotVersion::V1(testify_version::<_, 1>(PhantomData))),
                2 => Ok(SnapshotVersion::V2(testify_version::<_, 2>(PhantomData))),
                3 => Ok(SnapshotVersion::V3(testify_version::<_, 3>(PhantomData))),
                4 => Ok(SnapshotVersion::V4(testify_version::<_, 4>(PhantomData))),
                5 => Ok(SnapshotVersion::V5(testify_version::<_, 5>(PhantomData))),
                _ => Err(ErrorCode::Internal(format!(
                    "unknown snapshot segment version {value}, versions supported: 0, 1, 2, 3, 4, 5"
                ))),
            }
        }
    }

    impl TryFrom<u64> for TableSnapshotStatisticsVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(TableSnapshotStatisticsVersion::V0(testify_version::<_, 0>(
                    PhantomData,
                ))),
                2 => Ok(TableSnapshotStatisticsVersion::V2(testify_version::<_, 2>(
                    PhantomData,
                ))),
                3 => Ok(TableSnapshotStatisticsVersion::V3(testify_version::<_, 3>(
                    PhantomData,
                ))),
                _ => Err(ErrorCode::Internal(format!(
                    "unknown table snapshot statistics version {value}, versions supported: 0"
                ))),
            }
        }
    }
}
