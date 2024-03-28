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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key as tk;
use databend_common_meta_app::tenant_key::TenantResource;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_types::NonEmptyString;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl<R> FromToProto for tk::TIdent<R>
where R: TenantResource
{
    type PB = pb::TIdent;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TIdent) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let non_empty = NonEmptyString::new(p.tenant.clone())
            .map_err(|_e| Incompatible::new("tenant is empty"))?;

        let tenant = Tenant::new_nonempty(non_empty);

        let v = Self::new(tenant, p.name);
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TIdent, Incompatible> {
        let p = pb::TIdent {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            tenant: self.tenant_name().to_string(),
            name: self.name().to_string(),
        };
        Ok(p)
    }
}
