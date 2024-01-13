// Copyright 2020-2022 Jorge C. Leitão
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

#[cfg(feature = "serde_types")]
use serde_derive::Deserialize;
#[cfg(feature = "serde_types")]
use serde_derive::Serialize;

use super::DataType;
use super::Metadata;

/// Represents Arrow's metadata of a "column".
///
/// A [`Field`] is the closest representation of the traditional "column": a logical type
/// ([`DataType`]) with a name and nullability.
/// A Field has optional [`Metadata`] that can be used to annotate the field with custom metadata.
///
/// Almost all IO in this crate uses [`Field`] to represent logical information about the data
/// to be serialized.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde_types", derive(Serialize, Deserialize))]
pub struct Field {
    /// Its name
    pub name: String,
    /// Its logical [`DataType`]
    pub data_type: DataType,
    /// Its nullability
    pub is_nullable: bool,
    /// Additional custom (opaque) metadata.
    pub metadata: Metadata,
}

impl Field {
    /// Creates a new [`Field`].
    pub fn new<T: Into<String>>(name: T, data_type: DataType, is_nullable: bool) -> Self {
        Field {
            name: name.into(),
            data_type,
            is_nullable,
            metadata: Default::default(),
        }
    }

    /// Creates a new [`Field`] with metadata.
    #[inline]
    pub fn with_metadata(self, metadata: Metadata) -> Self {
        Self {
            name: self.name,
            data_type: self.data_type,
            is_nullable: self.is_nullable,
            metadata,
        }
    }

    /// Returns the [`Field`]'s [`DataType`].
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[cfg(feature = "arrow")]
impl From<Field> for arrow_schema::Field {
    fn from(value: Field) -> Self {
        Self::new(value.name, value.data_type.into(), value.is_nullable)
            .with_metadata(value.metadata.into_iter().collect())
    }
}

#[cfg(feature = "arrow")]
impl From<arrow_schema::Field> for Field {
    fn from(value: arrow_schema::Field) -> Self {
        (&value).into()
    }
}

#[cfg(feature = "arrow")]
impl From<&arrow_schema::Field> for Field {
    fn from(value: &arrow_schema::Field) -> Self {
        let data_type = value.data_type().clone().into();
        let metadata = value
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Self::new(value.name(), data_type, value.is_nullable()).with_metadata(metadata)
    }
}

#[cfg(feature = "arrow")]
impl From<arrow_schema::FieldRef> for Field {
    fn from(value: arrow_schema::FieldRef) -> Self {
        value.as_ref().into()
    }
}

#[cfg(feature = "arrow")]
impl From<&arrow_schema::FieldRef> for Field {
    fn from(value: &arrow_schema::FieldRef) -> Self {
        value.as_ref().into()
    }
}
