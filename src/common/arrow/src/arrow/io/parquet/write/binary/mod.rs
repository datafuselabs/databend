mod basic;
mod nested;

pub use basic::array_to_page;
pub(crate) use basic::build_statistics;
pub(super) use basic::encode_delta;
pub(crate) use basic::encode_plain;
pub(super) use basic::ord_binary;
pub use nested::array_to_page as nested_array_to_page;
