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
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::Array;
use arrow_array::LargeListArray;
use arrow_array::MapArray;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Fields;
use arrow_schema::Schema as ArrowSchema;
use databend_common_arrow::arrow::datatypes::DataType as Arrow2DataType;
use databend_common_arrow::arrow::datatypes::Field as Arrow2Field;
use databend_common_exception::Result;

use super::EXTENSION_KEY;
use crate::infer_table_schema;
use crate::Column;
use crate::DataBlock;
use crate::DataField;
use crate::DataSchema;
use crate::TableField;
use crate::TableSchema;

impl From<&DataSchema> for ArrowSchema {
    fn from(schema: &DataSchema) -> Self {
        let fields = schema
            .fields
            .iter()
            .map(|f| arrow_field_from_arrow2_field(Arrow2Field::from(f)))
            .collect::<Vec<_>>();
        ArrowSchema {
            fields: Fields::from(fields),
            metadata: schema.metadata.clone().into_iter().collect(),
        }
    }
}

impl From<&TableSchema> for ArrowSchema {
    fn from(schema: &TableSchema) -> Self {
        let fields = schema
            .fields
            .iter()
            .map(|f| arrow_field_from_arrow2_field(Arrow2Field::from(f)))
            .collect::<Vec<_>>();
        ArrowSchema {
            fields: Fields::from(fields),
            metadata: schema.metadata.clone().into_iter().collect(),
        }
    }
}

pub fn table_schema_to_arrow_schema(schema: &TableSchema) -> ArrowSchema {
    let fields = schema
        .fields
        .iter()
        .map(|f| arrow_field_from_arrow2_field(f.into()))
        .collect::<Vec<_>>();
    ArrowSchema {
        fields: Fields::from(fields),
        metadata: schema.metadata.clone().into_iter().collect(),
    }
}

impl From<&TableField> for ArrowField {
    fn from(field: &TableField) -> Self {
        arrow_field_from_arrow2_field(Arrow2Field::from(field))
    }
}

impl From<&DataField> for ArrowField {
    fn from(field: &DataField) -> Self {
        arrow_field_from_arrow2_field(Arrow2Field::from(field))
    }
}

impl DataBlock {
    // Notice this function may loss some struct tuples as we are using infer_schema
    pub fn to_record_batch_with_dataschema(self, data_schema: &DataSchema) -> Result<RecordBatch> {
        let table_schema = infer_table_schema(data_schema)?;
        self.to_record_batch(&table_schema)
    }

    pub fn to_record_batch(self, table_schema: &TableSchema) -> Result<RecordBatch> {
        let arrow_schema = table_schema_to_arrow_schema(table_schema);
        let mut arrays = Vec::with_capacity(self.columns().len());
        for (entry, arrow_field) in self
            .consume_convert_to_full()
            .columns()
            .iter()
            .zip(arrow_schema.fields())
        {
            let column = entry.value.to_owned().into_column().unwrap();
            let array = column.into_arrow_rs();

            // Adjust struct array names
            arrays.push(Self::adjust_nested_array(array, arrow_field.as_ref()));
        }
        Ok(RecordBatch::try_new(Arc::new(arrow_schema), arrays)?)
    }

    fn adjust_nested_array(array: Arc<dyn Array>, arrow_field: &ArrowField) -> Arc<dyn Array> {
        if let ArrowDataType::Struct(fs) = arrow_field.data_type() {
            let array = array.as_ref().as_struct();
            let inner_arrays = array
                .columns()
                .iter()
                .zip(fs.iter())
                .map(|(array, arrow_field)| {
                    Self::adjust_nested_array(array.clone(), arrow_field.as_ref())
                })
                .collect();

            let array = StructArray::new(fs.clone(), inner_arrays, array.nulls().cloned());
            Arc::new(array) as _
        } else if let ArrowDataType::LargeList(f) = arrow_field.data_type() {
            let array = array.as_ref().as_list::<i64>();
            let values = Self::adjust_nested_array(array.values().clone(), f.as_ref());
            let array = LargeListArray::new(
                f.clone(),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            );
            Arc::new(array) as _
        } else if let ArrowDataType::Map(f, ordered) = arrow_field.data_type() {
            let array = array.as_ref().as_map();

            let entry = Arc::new(array.entries().clone()) as Arc<dyn Array>;
            let entry = Self::adjust_nested_array(entry, f.as_ref());

            let array = MapArray::new(
                f.clone(),
                array.offsets().clone(),
                entry.as_struct().clone(),
                array.nulls().cloned(),
                *ordered,
            );
            Arc::new(array) as _
        } else {
            array
        }
    }
}

impl Column {
    pub fn into_arrow_rs(self) -> Arc<dyn arrow_array::Array> {
        let arrow2_array: Box<dyn databend_common_arrow::arrow::array::Array> = self.as_arrow();
        let arrow_array: Arc<dyn arrow_array::Array> = arrow2_array.into();
        arrow_array
    }
}

fn arrow_field_from_arrow2_field(field: Arrow2Field) -> ArrowField {
    let mut metadata = HashMap::new();

    let arrow2_data_type = if let Arrow2DataType::Extension(extension_type, ty, _) = field.data_type
    {
        metadata.insert(EXTENSION_KEY.to_string(), extension_type.clone());
        *ty
    } else {
        field.data_type
    };

    let data_type = match arrow2_data_type {
        Arrow2DataType::List(f) => ArrowDataType::List(Arc::new(arrow_field_from_arrow2_field(*f))),
        Arrow2DataType::LargeList(f) => {
            ArrowDataType::LargeList(Arc::new(arrow_field_from_arrow2_field(*f)))
        }
        Arrow2DataType::FixedSizeList(f, size) => {
            ArrowDataType::FixedSizeList(Arc::new(arrow_field_from_arrow2_field(*f)), size as _)
        }
        Arrow2DataType::Map(f, ordered) => {
            ArrowDataType::Map(Arc::new(arrow_field_from_arrow2_field(*f)), ordered)
        }
        Arrow2DataType::Struct(f) => {
            ArrowDataType::Struct(f.into_iter().map(arrow_field_from_arrow2_field).collect())
        }
        other => other.into(),
    };

    ArrowField::new(field.name, data_type, field.is_nullable).with_metadata(metadata)
}
