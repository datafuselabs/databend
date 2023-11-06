use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;
use std::fmt::Write;

use super::super::fmt::get_display;
use super::super::fmt::write_vec;
use super::FixedSizeListArray;

pub fn write_value<W: Write>(
    array: &FixedSizeListArray,
    index: usize,
    null: &'static str,
    f: &mut W,
) -> Result {
    let values = array.value(index);
    let writer = |f: &mut W, index| get_display(values.as_ref(), null)(f, index);
    write_vec(f, writer, None, values.len(), null, false)
}

impl Debug for FixedSizeListArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let writer = |f: &mut Formatter, index| write_value(self, index, "None", f);

        write!(f, "FixedSizeListArray")?;
        write_vec(f, writer, self.validity(), self.len(), "None", false)
    }
}
