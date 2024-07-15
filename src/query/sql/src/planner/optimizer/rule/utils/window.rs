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

use std::cmp::min;
use crate::plans::Window;

pub(crate) fn merge_window_op(up_window: Window, down_window: Window) -> Option<Window> {
    if up_window.partition_by != down_window.partition_by {
        return None;
    }
    let len = min(up_window.order_by.len(), down_window.order_by.len());
    if up_window.order_by[..len] != down_window.order_by[..len] {
        return None;
    }
    // TODO: support merge if frame is not equal
    if up_window.frame != down_window.frame {
        return None;
    }

    let index = up_window
        .index
        .into_iter()
        .chain(down_window.index)
        .collect();
    let function = up_window
        .function
        .into_iter()
        .chain(down_window.function)
        .collect();
    let arguments = up_window
        .arguments
        .into_iter()
        .chain(down_window.arguments)
        .collect();
    let order_by = if up_window.order_by.len() > down_window.order_by.len() {
        up_window.order_by
    } else {
        down_window.order_by
    };

    Some(Window {
        span: up_window.span,
        index,
        function,
        arguments,
        partition_by: up_window.partition_by,
        order_by,
        frame: up_window.frame.clone(),
        limit: None,
    })
}