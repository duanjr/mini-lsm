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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

use iterator::INVALID_BLOCK_IDX;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            buf.extend_from_slice(&(meta.offset as u16).to_le_bytes());
            buf.extend_from_slice(&(meta.first_key.len() as u16).to_le_bytes());
            buf.extend_from_slice(meta.first_key.raw_ref());
            buf.extend_from_slice(&(meta.last_key.len() as u16).to_le_bytes());
            buf.extend_from_slice(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_metas = Vec::new();
        let mut buf = buf;
        while buf.has_remaining() {
            let offset = buf.get_u16_le() as usize;
            let first_key_len = buf.get_u16_le() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len = buf.get_u16_le() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_metas.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let data = file.read(0, file.size())?;
        let block_meta_offset = u32::from_le_bytes(data[data.len() - 4..].try_into()?) as usize;
        let block_meta_data = &data[block_meta_offset..data.len() - 4];
        let block_meta = BlockMeta::decode_block_meta(block_meta_data);
        if block_meta.is_empty() {
            return Err(anyhow::anyhow!("No block meta found in the SSTable"));
        }

        let first_key = block_meta
            .first()
            .map_or(KeyBytes::default(), |meta| meta.first_key.clone());
        let last_key = block_meta
            .last()
            .map_or(KeyBytes::default(), |meta| meta.last_key.clone());

        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None, // TODO: Implement bloom filter
            max_ts: 0,   // TODO: Implement max timestamp tracking
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let data = self.file.read(0, self.file.size())?;
        let block_meta_offset = u32::from_le_bytes(data[data.len() - 4..].try_into()?) as usize;
        let block_meta_data = &data[block_meta_offset..data.len() - 4];
        let block_meta = BlockMeta::decode_block_meta(block_meta_data);
        if block_meta.is_empty() {
            return Err(anyhow::anyhow!("No block meta found in the SSTable"));
        }
        if block_idx >= block_meta.len() {
            return Err(anyhow::anyhow!("Block index out of bounds"));
        }

        let lower_bound = block_meta[block_idx].offset;
        let upper_bound = if block_idx == block_meta.len() - 1 {
            block_meta_offset
        } else {
            block_meta[block_idx + 1].offset
        };

        Ok(Arc::new(Block::decode(&data[lower_bound..upper_bound])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if self.block_cache.is_none() {
            return self.read_block(block_idx);
        }
        let block_cache = self.block_cache.as_ref().unwrap();

        let result = block_cache.try_get_with((self.id, block_idx), || self.read_block(block_idx));
        match result {
            Ok(block) => Ok(block),
            Err(err) => Err(anyhow::anyhow!("Failed to read block from cache: {}", err)),
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let data = self.file.read(0, self.file.size()).unwrap();
        let block_meta_offset =
            u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
        let block_meta_data = &data[block_meta_offset..data.len() - 4];
        let block_meta = BlockMeta::decode_block_meta(block_meta_data);
        if block_meta.is_empty() {
            return INVALID_BLOCK_IDX; // No valid block found
        }
        let (mut low, mut high) = (0, block_meta.len() - 1);
        while low < high {
            let mid = (low + high) / 2;
            if block_meta[mid].last_key.as_key_slice() >= key {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        if low >= block_meta.len() {
            return INVALID_BLOCK_IDX; // No valid block found
        }
        low
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
