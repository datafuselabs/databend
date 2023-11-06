use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;

use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::Utf8Array;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::offset::Offset;
use crate::arrow::offset::OffsetsBuffer;

impl<O: Offset> Arrow2Arrow for Utf8Array<O> {
    fn to_data(&self) -> ArrayData {
        let data_type = self.data_type().clone().into();
        let builder = ArrayDataBuilder::new(data_type)
            .len(self.offsets().len_proxy())
            .buffers(vec![
                self.offsets.clone().into_inner().into(),
                self.values.clone().into(),
            ])
            .nulls(self.validity.as_ref().map(|b| b.clone().into()));

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let data_type = data.data_type().clone().into();
        if data.is_empty() {
            // Handle empty offsets
            return Self::new_empty(data_type);
        }

        let buffers = data.buffers();

        // Safety: ArrayData is valid
        let mut offsets = unsafe { OffsetsBuffer::new_unchecked(buffers[0].clone().into()) };
        offsets.slice(data.offset(), data.len() + 1);

        Self {
            data_type,
            offsets,
            values: buffers[1].clone().into(),
            validity: data.nulls().map(|n| Bitmap::from_null_buffer(n.clone())),
        }
    }
}
