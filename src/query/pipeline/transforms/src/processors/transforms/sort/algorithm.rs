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

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::loser_tree;
use super::utils::find_bigger_child_of_root;
use super::Cursor;
use super::Rows;

pub trait SortdGroup
where <Self as SortdGroup>::Rows: Rows
{
    type Rows;
    fn with_capacity(capacity: usize) -> Self;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize;

    fn push(&mut self, index: usize, item: Reverse<Cursor<Self::Rows>>);

    fn pop(&mut self);

    fn update_top(&mut self, item: Reverse<Cursor<Self::Rows>>);

    fn peek(&self) -> Option<&Reverse<Cursor<Self::Rows>>>;

    fn peek_top2(&self) -> &Reverse<Cursor<Self::Rows>>;
}

pub type BinaryHeapSort<R> = BinaryHeap<Reverse<Cursor<R>>>;

impl<R: Rows> SortdGroup for BinaryHeap<Reverse<Cursor<R>>> {
    type Rows = R;
    fn with_capacity(capacity: usize) -> Self {
        BinaryHeap::with_capacity(capacity)
    }

    fn len(&self) -> usize {
        BinaryHeap::len(self)
    }

    fn push(&mut self, _index: usize, item: Reverse<Cursor<Self::Rows>>) {
        BinaryHeap::push(self, item)
    }

    fn pop(&mut self) {
        BinaryHeap::pop(self);
    }

    fn update_top(&mut self, item: Reverse<Cursor<Self::Rows>>) {
        *BinaryHeap::peek_mut(self).unwrap() = item
    }

    fn peek(&self) -> Option<&Reverse<Cursor<Self::Rows>>> {
        BinaryHeap::peek(self)
    }

    fn peek_top2(&self) -> &Reverse<Cursor<Self::Rows>> {
        find_bigger_child_of_root(self)
    }
}

pub struct LoserTreeSort<R: Rows> {
    tree: loser_tree::LoserTree<Option<Reverse<Cursor<R>>>>,
    length: usize,
}

impl<R: Rows> SortdGroup for LoserTreeSort<R> {
    type Rows = R;
    fn with_capacity(capacity: usize) -> Self {
        let data = vec![None; capacity];
        LoserTreeSort {
            tree: loser_tree::LoserTree::from(data),
            length: 0,
        }
    }

    fn len(&self) -> usize {
        self.length
    }

    fn push(&mut self, index: usize, item: Reverse<Cursor<Self::Rows>>) {
        self.tree.update(index, Some(item));
        self.length += 1
    }

    fn pop(&mut self) {
        *self.tree.peek_mut() = None;
        self.length -= 1;
    }

    fn update_top(&mut self, item: Reverse<Cursor<Self::Rows>>) {
        *self.tree.peek_mut() = Some(item)
    }

    fn peek(&self) -> Option<&Reverse<Cursor<Self::Rows>>> {
        self.tree.peek().as_ref()
    }

    fn peek_top2(&self) -> &Reverse<Cursor<Self::Rows>> {
        self.tree.peek_top2().as_ref().unwrap()
    }
}
