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

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::processors::Event;
use crate::processors::InputPort;
use crate::processors::OutputPort;
use crate::processors::Processor;

pub struct DuplicateProcessor {
    input: Arc<InputPort>,
    outputs: Vec<Arc<OutputPort>>,

    /// Whether all outputs should finish together.
    force_finish_together: bool,
}

/// This processor duplicate the input data to multiple outputs.
impl DuplicateProcessor {
    pub fn create(
        input: Arc<InputPort>,
        outputs: Vec<Arc<OutputPort>>,
        force_finish_together: bool,
    ) -> Self {
        DuplicateProcessor {
            input,
            outputs,
            force_finish_together,
        }
    }
}

#[async_trait::async_trait]
impl Processor for DuplicateProcessor {
    fn name(&self) -> String {
        "Duplicate".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let all_finished = self.outputs.iter().all(|x| x.is_finished());

        if all_finished
            || (self.force_finish_together && self.outputs.iter().any(|x| x.is_finished()))
        {
            self.input.finish();
            self.outputs.iter_mut().for_each(|x| x.finish());
            return Ok(Event::Finished);
        }

        if self.input.is_finished() {
            self.outputs.iter_mut().for_each(|x| x.finish());
            return Ok(Event::Finished);
        }

        if self
            .outputs
            .iter()
            .any(|x| !x.is_finished() && !x.can_push())
        {
            return Ok(Event::NeedConsume);
        }

        self.input.set_need_data();
        if self.input.has_data() {
            let block = self.input.pull_data().unwrap();
            for output in self.outputs.iter() {
                if !output.is_finished() {
                    output.push_data(block.clone());
                }
            }
            return Ok(Event::NeedConsume);
        }

        Ok(Event::NeedData)
    }
}
