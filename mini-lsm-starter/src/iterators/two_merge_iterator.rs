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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    cursor_on_a: bool, // true if the cursor is on A, false if on B
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let cursor_on_a = !(!a.is_valid() || (b.is_valid() && b.key() < a.key()));
        Ok(Self { a, b, cursor_on_a })
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.cursor_on_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.cursor_on_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.cursor_on_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            // If keys are equal, we prefer the value from A
            self.b.next()?;
        }

        if self.cursor_on_a {
            if self.a.is_valid() {
                self.a.next()?;
            }
            if !self.a.is_valid() || (self.b.is_valid() && self.b.key() < self.a.key()) {
                self.cursor_on_a = false;
            }
        } else {
            if self.b.is_valid() {
                self.b.next()?;
            }
            if !self.b.is_valid() || (self.a.is_valid() && self.a.key() <= self.b.key()) {
                self.cursor_on_a = true;
            }
        }
        Ok(())
    }
}
