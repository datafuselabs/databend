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

use databend_common_meta_types::NodeInfo;

#[test]
fn test_node_info_ip_port() -> anyhow::Result<()> {
    let n = NodeInfo::create(
        "".to_string(),
        "".to_string(),
        1,
        "1.2.3.4:123".to_string(),
        "v0.8-binary-version".to_string(),
    );

    let (ip, port) = n.ip_port()?;
    assert_eq!("1.2.3.4".to_string(), ip);
    assert_eq!(123, port);
    assert_eq!("v0.8-binary-version".to_string(), n.binary_version);

    Ok(())
}
