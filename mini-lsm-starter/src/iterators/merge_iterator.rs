// Copyright (c) 2022-2025 Alex Chi Z
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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::mem::swap;

use anyhow::Result;
use nom::AsBytes;

use crate::key::{Key, KeySlice};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut new_iters: BinaryHeap<HeapWrapper<I>> = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if !iter.is_valid() {
                continue;
            }
            new_iters.push(HeapWrapper(i, iter));
        }
        let init_iter = new_iters.pop();
        MergeIterator {
            iters: new_iters,
            current: init_iter,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if let Some(heap_wrapper) = &self.current {
            return heap_wrapper.1.key();
        }
        Key(&[])
    }

    fn value(&self) -> &[u8] {
        if let Some(heap_wrapper) = &self.current {
            return heap_wrapper.1.value().as_bytes();
        }
        &[]
    }

    fn is_valid(&self) -> bool {
        match self.current.as_ref() {
            None => false,
            Some(current_iter) => current_iter.1.is_valid(),
        }
    }

    /// Reference: https://github.com/skyzh/mini-lsm-solution-checkpoint/commit/8e828d72badceb9b9255345da2c9bf247a0f15b1
    fn next(&mut self) -> Result<()> {
        let current_iter = match self.current.as_mut() {
            None => return Ok(()),
            Some(heap_wrapper) => heap_wrapper,
        };

        while let Some(mut next_iter) = self.iters.peek_mut() {
            if next_iter.1.key() == current_iter.1.key() {
                if let Err(e) = next_iter.1.next() {
                    PeekMut::pop(next_iter);
                    return Err(e);
                }
                if !next_iter.1.is_valid() {
                    PeekMut::pop(next_iter);
                }
            } else {
                break;
            }
        }

        current_iter.1.next()?;

        if !current_iter.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current_iter = iter;
            }
            return Ok(());
        }
        // Compare and swap(maybe) with heap top
        if let Some(mut next_iter) = self.iters.peek_mut() {
            if *current_iter < *next_iter {
                swap(current_iter, &mut next_iter);
            }
        }

        Ok(())
    }
}
