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

// https://github.com/rust-lang/rust-clippy/issues/8334
#![allow(clippy::ptr_arg)]
#![allow(clippy::uninlined_format_args)]
#![allow(internal_features)]
#![feature(can_vector)]
#![feature(read_buf)]
#![feature(slice_internals)]
#![feature(maybe_uninit_slice)]
#![feature(new_uninit)]
#![feature(cursor_remaining)]
#![feature(buf_read_has_data_left)]

pub mod constants;
pub mod format_diagnostic;
pub mod prelude;

mod binary_read;
mod binary_write;

mod bincode_serialization;
mod bitmap;
mod borsh_serialization;
pub mod cursor_ext;
mod decimal;
mod escape;
mod format_settings;
mod geography;
mod geometry;
mod position;
mod stat_buffer;

pub use bitmap::deserialize_bitmap;
pub use bitmap::parse_bitmap;
pub use decimal::display_decimal_128;
pub use decimal::display_decimal_256;
pub use escape::escape_string;
pub use escape::escape_string_with_quote;
pub use geography::parse_geometry;
pub use geometry::geometry_format;
pub use geometry::parse_to_ewkb;
pub use geometry::parse_to_subtype;
pub use geometry::Axis;
pub use geometry::Extremum;
pub use geometry::GeometryDataType;
