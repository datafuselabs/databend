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

use databend_common_exception::Result;
use databend_common_sql::executor::physical_plans::MergeIntoOp;
use databend_common_sql::executor::physical_plans::MergeIntoShuffle;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // Shuffle outputs and resize row_id
    pub(crate) fn build_merge_into_shuffle(
        &mut self,
        merge_into_shuffle: &MergeIntoShuffle,
    ) -> Result<()> {
        self.build_pipeline(&merge_into_shuffle.input)?;

        // ------------------------------Standalone-------------------------------------------------
        // row_id port0_1               row_id port0_1              row_id port0_1
        // matched data port0_2              .....                  row_id port1_1         row_id port
        // unmatched port0_3            data port0_2                    ......
        // row_id port1_1       ====>   row_id port1_1    ====>     data port0_2    ====>  data port0
        // matched data port1_2              .....                  data port1_2           data port1
        // unmatched port1_3            data port1_2                    ......
        // ......                            .....
        // -----------------------------------------------------------------------------------------
        // 1. matched only or complete pipeline are same with above
        // 2. for unmatched only/insert only, there are no row_id port

        // ---------------------Distributed(change_join_order = false)------------
        // row_id port0_1              row_id port0_1              row_id port
        // matched data port0_2        row_id port1_1            matched data port0_2
        // row_number port0_3          matched data port0_2      matched data port1_2
        // row_id port1_1              matched data port1_2          ......
        // matched data port1_2  ===>       .....           ====>    ......
        // row_number port1_3               .....                    ......
        //                           row_number port0_3          row_number port
        // ......                    row_number port1_3
        // ......                           .....
        // ----------------------------------------------------------------------
        // 1.for matched only, there is no row_number port
        // 2.for unmatched only/insert only, there is no row_id port and matched data port

        // ---------------------Distributed(change_join_order = true)------------
        // row_id port0_1              row_id port0_1              row_id port
        // matched data port0_2        row_id port1_1            matched data port0_2
        // unmatched port0_3           matched data port0_2      matched data port1_2
        // row_id port1_1              matched data port1_2          ......
        // matched data port1_2  ===>       .....           ====>    ......
        // unmatched port1_3                .....                    ......
        //                            unmatched port0_3          unmatched port
        // ......                     unmatched port1_3
        // ......                           .....
        // ----------------------------------------------------------------------
        // 1.for matched only, there is no unmatched port
        // 2.for unmatched only/insert only, there is no row_id port and matched data port

        let mut ranges = Vec::with_capacity(self.main_pipeline.output_len());
        let mut rules = Vec::with_capacity(self.main_pipeline.output_len());

        match merge_into_shuffle.merge_into_op {
            MergeIntoOp::StandaloneFullOperation => {
                assert_eq!(self.main_pipeline.output_len() % 3, 0);
                // merge matched update ports and not matched ports ===> data ports
                for idx in (0..self.main_pipeline.output_len()).step_by(3) {
                    ranges.push(vec![idx]);
                    ranges.push(vec![idx + 1, idx + 2]);
                }
                self.main_pipeline.resize_partial_one(ranges.clone())?;
                assert_eq!(self.main_pipeline.output_len() % 2, 0);
                let shuffle_len = self.main_pipeline.output_len() / 2;
                for idx in 0..shuffle_len {
                    rules.push(idx);
                    rules.push(idx + shuffle_len);
                }
                self.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(2)?;
            }
            MergeIntoOp::StandaloneMatchedOnly => {
                let shuffle_len = self.main_pipeline.output_len() / 2;
                for idx in 0..shuffle_len {
                    rules.push(idx);
                    rules.push(idx + shuffle_len);
                }
                self.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(2)?;
            }
            MergeIntoOp::StandaloneInsertOnly => {}
            MergeIntoOp::DistributedFullOperation => {
                let shuffle_len = self.main_pipeline.output_len() / 2;
                for idx in 0..shuffle_len {
                    rules.push(idx);
                    rules.push(idx + shuffle_len);
                    rules.push(idx + shuffle_len * 2);
                }
                self.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(3)?;
            }
            MergeIntoOp::DistributedMatchedOnly => {
                let shuffle_len = self.main_pipeline.output_len() / 2;
                for idx in 0..shuffle_len {
                    rules.push(idx);
                    rules.push(idx + shuffle_len);
                }
                self.main_pipeline.reorder_inputs(rules);
                self.resize_row_id(2)?;
            }
            MergeIntoOp::DistributedInsertOnly => {
                // insert-only, there are only row_number ports/unmatched ports
                self.main_pipeline.try_resize(1)?;
            }
        }
        Ok(())
    }

    fn resize_row_id(&mut self, step: usize) -> Result<()> {
        // resize row_id
        let resize_len = self.main_pipeline.output_len() / step;
        let mut ranges = Vec::with_capacity(self.main_pipeline.output_len());
        let mut vec = Vec::with_capacity(resize_len);
        for idx in 0..resize_len {
            vec.push(idx);
        }
        ranges.push(vec.clone());

        // Standalone: data port(matched update port and unmatched  port)
        // Distributed: matched update port
        for idx in 0..resize_len {
            ranges.push(vec![idx + resize_len]);
        }

        // Distributed: need to resize row_number port/unmatched data port.
        if step == 3 {
            vec.clear();
            for idx in 0..resize_len {
                vec.push(idx + resize_len * 2);
            }
            ranges.push(vec);
        }

        self.main_pipeline.resize_partial_one(ranges.clone())
    }
}
