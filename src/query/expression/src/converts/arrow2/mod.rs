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

mod from;
mod to;

pub const ARROW_EXT_TYPE_EMPTY_ARRAY: &str = "EmptyArray";
pub const ARROW_EXT_TYPE_EMPTY_MAP: &str = "EmptyMap";
pub const ARROW_EXT_TYPE_VARIANT: &str = "Variant";
pub const ARROW_EXT_TYPE_BITMAP: &str = "Bitmap";
pub const ARROW_EXT_TYPE_GEOMETRY: &str = "Geometry";
pub const ARROW_EXT_TYPE_GEOGRAPHY: &str = "Geography";

pub use to::set_validities;
