// Copyright [2021] [Jorge C Leitao]
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

use crate::encoding::delta_bitpacked;

/// Encodes a cloneable iterator of `&[u8]` into `buffer`. This does not allocated on the heap.
/// # Implementation
/// This encoding is equivalent to call [`delta_bitpacked::encode`] on the lengths of the items
/// of the iterator followed by extending the buffer from each item of the iterator.
pub fn encode<A: AsRef<[u8]>, I: Iterator<Item = A> + Clone>(iterator: I, buffer: &mut Vec<u8>) {
    let mut total_length = 0;
    delta_bitpacked::encode(
        iterator.clone().map(|x| {
            let len = x.as_ref().len();
            total_length += len;
            len as i64
        }),
        buffer,
    );
    buffer.reserve(total_length);
    iterator.for_each(|x| buffer.extend(x.as_ref()))
}
