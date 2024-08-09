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

use std::fmt;
use std::fmt::Formatter;

use databend_common_meta_types::MatchSeq;

use crate::tenant_key::resource::TenantResource;

/// Error occurred when a record already exists for a key.
#[derive(Clone, PartialEq, Eq, thiserror::Error)]
pub struct ExistError<R> {
    name: String,
    ctx: String,
    _p: std::marker::PhantomData<R>,
}

impl<R> ExistError<R> {
    pub fn new(name: impl ToString, ctx: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            ctx: ctx.to_string(),
            _p: Default::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ctx(&self) -> &str {
        &self.ctx
    }
}

impl<R> fmt::Debug for ExistError<R>
where R: TenantResource
{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let typ = type_name::<R>();

        f.debug_struct("ExistError")
            .field("type", &typ)
            .field("name", &self.name)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<R> fmt::Display for ExistError<R>
where R: TenantResource
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let typ = type_name::<R>();
        write!(f, "{typ} '{}' already exists: {}", self.name, self.ctx)
    }
}

/// Error occurred when a record not found for a key.
#[derive(Clone, PartialEq, Eq, thiserror::Error)]
pub struct UnknownError<R> {
    name: String,
    match_seq: MatchSeq,
    ctx: String,
    _p: std::marker::PhantomData<R>,
}

impl<R> UnknownError<R> {
    pub fn new(name: impl ToString, match_seq: MatchSeq, ctx: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            match_seq,
            ctx: ctx.to_string(),
            _p: Default::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ctx(&self) -> &str {
        &self.ctx
    }
}

impl<R> fmt::Debug for UnknownError<R>
where R: TenantResource
{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let typ = type_name::<R>();

        f.debug_struct("UnknownError")
            .field("type", &typ)
            .field("name", &self.name)
            .field("match_seq", &self.match_seq)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<R> fmt::Display for UnknownError<R>
where R: TenantResource
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let typ = type_name::<R>();
        write!(
            f,
            "Unknown {typ} '{}'(seq {}): {}",
            self.name, self.match_seq, self.ctx
        )
    }
}

fn type_name<R: TenantResource>() -> &'static str {
    let full_name = std::any::type_name::<R::ValueType>();
    let mut segs = full_name.rsplitn(2, "::");
    segs.next().unwrap()
}

#[cfg(test)]
mod tests {
    use databend_common_meta_types::MatchSeq;

    use crate::principal::network_policy_ident;

    #[test]
    fn test_exist_error() {
        let err = super::ExistError::<network_policy_ident::Resource> {
            name: "foo".to_string(),
            ctx: "bar".to_string(),
            _p: Default::default(),
        };

        let got = err.to_string();
        assert_eq!(got, "NetworkPolicy 'foo' already exists: bar")
    }

    #[test]
    fn test_unknown_error() {
        let err = super::UnknownError::<network_policy_ident::Resource> {
            name: "foo".to_string(),
            match_seq: MatchSeq::GE(1),
            ctx: "bar".to_string(),
            _p: Default::default(),
        };

        let got = err.to_string();
        assert_eq!(got, "Unknown NetworkPolicy 'foo'(seq >= 1): bar")
    }
}
