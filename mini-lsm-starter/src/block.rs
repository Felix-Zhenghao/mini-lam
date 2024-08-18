mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

use crate::key::{Key, KeySlice};

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    // an entry = 2 bytes for key_len, 2 bytes for value_len, key_len bytes for key, value_len bytes for value
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }

        buf.put_u16(offsets_len as u16); // the num_of_elements field
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - SIZEOF_U16 * entry_offsets_len;
        let offsets: Vec<u16> = (&data[data_end..data.len() - SIZEOF_U16])
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let data = data[0..data_end].to_vec();
        Self {
            data: data,
            offsets: offsets,
        }
    }

    // utility func for building SsTable from blocks
    pub fn get_first_key(&self) -> &[u8] {
        let key_len = (&self.data[..SIZEOF_U16]).get_u16() as usize;
        &self.data[SIZEOF_U16..SIZEOF_U16 + key_len]
    }

    // utility func for building SsTable from blocks
    pub fn get_last_key(&self) -> &[u8] {
        let offset = *self.offsets.last().unwrap() as usize;
        let key_len = (&self.data[offset..offset + SIZEOF_U16]).get_u16() as usize;
        &self.data[offset + SIZEOF_U16..offset + SIZEOF_U16 + key_len]
    }
}
