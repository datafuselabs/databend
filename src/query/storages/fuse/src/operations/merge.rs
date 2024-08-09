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

use std::sync::Arc;

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_core::PipeItem;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use super::merge_into::MatchedAggregator;
use super::mutation::SegmentIndex;
use crate::io::create_inverted_index_builders;
use crate::io::BlockBuilder;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

impl FuseTable {
    // todo: (JackTan25) add pipeline picture
    // The big picture of the merge into pipeline:
    //
    //                                                                                                                                                +-------------------+
    //                                                                                         +-----------------------------+    output_port_row_id  |                   |
    //                                            +-----------------------+     Matched        |                             +------------------------>-ResizeProcessor(1)+---------------+
    //                                            |                       +---+--------------->|    MatchedSplitProcessor    |                        |                   |               |
    //                                            |                       |   |                |                             +----------+             +-------------------+               |
    //         +----------------------+           |                       +---+                +-----------------------------+          |                                                 |
    //         |       MergeInto      +---------->|MutationSplitProcessor |                                                       output_port_updated                                     |
    //         +----------------------+           |                       +---+                +-----------------------------+          |             +-------------------+               |
    //                                            |                       |   | NotMatched     |                             |          |             |                   |               |
    //                                            |                       +---+--------------->| MergeIntoNotMatchedProcessor+----------+------------->-ResizeProcessor(1)+-----------+   |
    //                                            +-----------------------+                    |                             |                        |                   |           |   |
    //                                                                                         +-----------------------------+                        +-------------------+           |   |
    //                                                                                                                                                                                |   |
    //                                                                                                                                                                                |   |
    //                                                                                                                                                                                |   |
    //                                                                                                                                                                                |   |
    //                                                                                                                                                                                |   |
    //                                                                               +-------------------------------------------------+                                              |   |
    //                                                                               |                                                 |                                              |   |
    //                                                                               |                                                 |                                              |   |
    //           +--------------------------+        +-------------------------+     |         ++---------------------------+          |     +--------------------------------------+ |   |
    // +---------+ TransformSerializeSegment<--------+ TransformSerializeBlock <-----+---------+|TransformAddComputedColumns|<---------+-----+TransformResortAddOnWithoutSourceSchema<-+   |
    // |         +--------------------------+        +-------------------------+     |         ++---------------------------+          |     +--------------------------------------+     |
    // |                                                                             |                                                 |                                                  |
    // |                                                                             |                                                 |                                                  |
    // |                                                                             |                                                 |                                                  |
    // |                                                                             |                                                 |                                                  |
    // |          +---------------+                 +------------------------------+ |               ++---------------+                |               +---------------+                  |
    // +----------+ TransformDummy|<----------------+ AsyncAccumulatingTransformer <-+---------------+|TransformDummy |<---------------+---------------+TransformDummy <------------------+
    // |          +---------------+                 +------------------------------+ |               ++---------------+                |               +---------------+
    // |                                                                             |                                                 |
    // |                                                                             |  If it includes 'computed', this section        |
    // |                                                                             |  of code will be executed, otherwise it won't   |
    // |                                                                             |                                                 |
    // |                                                                            -+-------------------------------------------------+
    // |
    // |
    // |
    // |        +------------------+            +-----------------------+        +-----------+
    // +------->|ResizeProcessor(1)+----------->|TableMutationAggregator+------->|CommitSink |
    //          +------------------+            +-----------------------+        +-----------+
    pub fn rowid_aggregate_mutator(
        &self,
        ctx: Arc<dyn TableContext>,
        cluster_stats_gen: ClusterStatsGenerator,
        io_request_semaphore: Arc<Semaphore>,
        segment_locations: Vec<(SegmentIndex, Location)>,
        target_build_optimization: bool,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<PipeItem> {
        let new_schema: TableSchemaRef = self
            .schema_with_stream()
            .remove_virtual_computed_fields()
            .into();
        let bloom_columns_map = self
            .bloom_index_cols()
            .bloom_index_fields(new_schema.clone(), BloomIndex::supported_type)?;
        let inverted_index_builders = create_inverted_index_builders(&self.table_info.meta);

        let block_builder = BlockBuilder {
            ctx: ctx.clone(),
            meta_locations: self.meta_location_generator().clone(),
            source_schema: new_schema,
            write_settings: self.get_write_settings(),
            cluster_stats_gen,
            bloom_columns_map,
            inverted_index_builders,
            table_meta_timestamps,
        };
        let aggregator = MatchedAggregator::create(
            ctx,
            self,
            block_builder,
            io_request_semaphore,
            segment_locations,
            target_build_optimization,
        )?;
        Ok(aggregator.into_pipe_item())
    }
}
