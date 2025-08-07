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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut data = Vec::with_capacity(self.data.len() + 2 + 2 * self.offsets.len());
        data.extend_from_slice(&self.data);
        for &offset in &self.offsets {
            data.extend_from_slice(&offset.to_le_bytes());
        }
        data.extend_from_slice(&(self.offsets.len() as u16).to_le_bytes());
        Bytes::from(data)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_elements =
            u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]) as usize;
        let mut offsets = Vec::with_capacity(num_elements);
        for i in 0..num_elements {
            let offset = u16::from_le_bytes([
                data[data.len() - 2 - 2 * num_elements + 2 * i],
                data[data.len() - 1 - 2 * num_elements + 2 * i],
            ]);
            offsets.push(offset);
        }
        Self {
            data: data[..data.len() - 2 - 2 * num_elements].to_vec(),
            offsets,
        }
    }
}
