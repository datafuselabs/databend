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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::SeqV;
use crate::SeqValue;

/// `Change` describes a state transition: the states before and after an operation.
///
/// Note that a success add-operation has a None `prev`, and a maybe-Some `result`.
/// Because the inserted value may have already expired and will be deleted at once,
/// the `result` could also be possible to be None.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, derive_more::From)]
pub struct Change<T, ID = u64>
where ID: Clone + PartialEq
{
    /// identity of the resource that is changed.
    pub ident: Option<ID>,
    pub prev: Option<SeqV<T>>,
    pub result: Option<SeqV<T>>,
}

impl<T, ID> Change<T, ID>
where
    ID: Clone + PartialEq + Debug,
    T: Debug,
{
    pub fn new(prev: Option<SeqV<T>>, result: Option<SeqV<T>>) -> Self {
        Change {
            ident: None,
            prev,
            result,
        }
    }

    pub fn with_id(mut self, id: ID) -> Self {
        self.ident = Some(id);
        self
    }

    /// Maps `Option<SeqV<T>>` to `Option<U>` for `prev` and `result`.
    pub fn map<F, U>(self, f: F) -> (Option<U>, Option<U>)
    where F: Fn(SeqV<T>) -> U + Copy {
        (self.prev.map(f), self.result.map(f))
    }

    pub fn unwrap(self) -> (SeqV<T>, SeqV<T>) {
        (self.prev.unwrap(), self.result.unwrap())
    }

    /// Extract `prev.seq` and `result.seq`.
    pub fn unpack_seq(self) -> (Option<u64>, Option<u64>) {
        self.map(|x| x.seq)
    }

    /// Extract `prev.data` and `result.data`.
    pub fn unpack_data(self) -> (Option<T>, Option<T>) {
        self.map(|x| x.data)
    }

    pub fn is_changed(&self) -> bool {
        self.prev.seq() != self.result.seq()
    }

    /// Assumes it is a state transition of an add operation and return Ok if the add operation succeed.
    /// Otherwise it returns an error that is built by provided function.
    ///
    /// Note that a success add-operation has a None `prev`, and a maybe-Some `result`.
    /// Because the inserted value may have already expired and will be deleted at once,
    /// the `result` could also be possible to be None.
    #[allow(dead_code)]
    pub fn added_or_else<F, E>(self, make_err: F) -> Result<Option<SeqV<T>>, E>
    where F: FnOnce(SeqV<T>) -> E {
        let (prev, result) = self.unpack();
        if let Some(p) = prev {
            return Err(make_err(p));
        }

        Ok(result)
    }

    /// Return the `seq` in an Ok if an add operation succeed.
    /// Otherwise it returns an error that is built by provided function.
    pub fn added_seq_or_else<F, E>(self, make_err: F) -> Result<u64, E>
    where F: FnOnce(SeqV<T>) -> E {
        let (prev, result) = self.unpack();
        if let Some(p) = prev {
            return Err(make_err(p));
        }

        // result could be None if it expired.
        Ok(result.seq())
    }
}

impl<T, ID> Change<T, ID>
where ID: Clone + PartialEq
{
    /// Extract `prev` and `result`.
    pub fn unpack(self) -> (Option<SeqV<T>>, Option<SeqV<T>>) {
        (self.prev, self.result)
    }

    /// Assumes it is a state transition of a remove operation and return Ok if the succeeded.
    /// Otherwise it returns an error that is built by provided function with the `prev` value as argument.
    ///
    /// Note that a success remove has a Some `prev`, and a None `result`.
    pub fn removed_or_else<F, E>(self, make_err: F) -> Result<SeqV<T>, E>
    where F: FnOnce(Option<SeqV<T>>) -> E {
        let (prev, result) = self.unpack();
        if result.is_none() {
            if let Some(p) = prev {
                return Ok(p);
            }
        }
        Err(make_err(prev))
    }
}

impl<T, ID> Display for Change<T, ID>
where
    T: Debug + Clone + PartialEq,
    ID: Debug + Clone + PartialEq,
{
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "id: {:?}", self.ident)?;
        write!(f, "prev: {:?}", self.prev)?;
        write!(f, "result: {:?}", self.result)
    }
}
