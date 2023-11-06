use crate::arrow::array::Array;
use crate::arrow::array::FixedSizeBinaryArray;

pub(super) fn equal(lhs: &FixedSizeBinaryArray, rhs: &FixedSizeBinaryArray) -> bool {
    lhs.data_type() == rhs.data_type() && lhs.len() == rhs.len() && lhs.iter().eq(rhs.iter())
}
