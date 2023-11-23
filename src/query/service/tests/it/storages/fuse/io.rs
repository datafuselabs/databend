//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use common_exception::Result;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use uuid::Uuid;

#[test]
fn test_meta_locations() -> Result<()> {
    let test_prefix = "test_pref";
    let locs = TableMetaLocationGenerator::new(test_prefix.to_owned(), 1);
    let ((path, _ver), _id) = locs.gen_block_location();
    assert!(path.starts_with(test_prefix));
    let seg_loc = locs.gen_segment_info_location();
    assert!(seg_loc.starts_with(test_prefix));
    let uuid = Uuid::new_v4();
    let snapshot_loc = locs.gen_snapshot_location(&uuid, TableSnapshot::VERSION, None)?;
    assert!(snapshot_loc.starts_with(test_prefix));
    Ok(())
}
