use crate::contracts::Result;

use super::refusal;

pub(super) struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(super) fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| refusal("length overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| refusal("truncated message"))?;
        self.offset = end;
        Ok(bytes)
    }

    pub(super) fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub(super) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub(super) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(super) fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(super) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(super) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub(super) fn string(&mut self, maximum: usize, empty: bool) -> Result<String> {
        let remaining = &self.bytes[self.offset..];
        let length = remaining
            .iter()
            .take(maximum + 1)
            .position(|byte| *byte == 0)
            .ok_or_else(|| refusal("unterminated or oversized metadata name"))?;
        if length == 0 && !empty {
            return Err(refusal("empty metadata name"));
        }
        let bytes = self.take(length)?;
        self.take(1)?;
        let text = std::str::from_utf8(bytes).map_err(|_| refusal("invalid metadata encoding"))?;
        Ok(text.to_owned())
    }

    pub(super) fn namespace(&mut self) -> Result<String> {
        let value = self.string(63, true)?;
        Ok(if value.is_empty() {
            "pg_catalog".into()
        } else {
            value
        })
    }

    pub(super) fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(refusal("trailing message bytes"));
        }
        Ok(())
    }
}
