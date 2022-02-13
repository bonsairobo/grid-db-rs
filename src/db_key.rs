use crate::{Level, NoSharedAllocSerializer};

use core::ops::RangeInclusive;
use ilattice::glam::{IVec2, IVec3};
use ilattice::prelude::{Bounded, Extent, Morton2i32, Morton3i32};
use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

pub trait DbKey:
    Archive + Clone + Debug + Eq + Hash + Ord + Sized + Serialize<NoSharedAllocSerializer<8192>>
{
    type Coords;
    type SledKey: AsRef<[u8]>;

    fn as_sled_key(&self) -> Self::SledKey;
    fn from_sled_key(bytes: &[u8]) -> Self;

    fn extent_range(level: u8, extent: Extent<Self::Coords>) -> RangeInclusive<Self>;

    fn min_key(level: u8) -> Self;
    fn max_key(level: u8) -> Self;
}

#[derive(
    Archive, Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize,
)]
#[archive_attr(derive(Debug, Eq, Hash, PartialEq, PartialOrd, Ord))]
pub struct DbKey2i32 {
    pub level: Level,
    pub morton: Morton2i32,
}

impl DbKey2i32 {
    pub fn new(level: Level, morton: Morton2i32) -> Self {
        Self { level, morton }
    }
}

impl DbKey for DbKey2i32 {
    type Coords = IVec2;
    type SledKey = [u8; 9];

    /// We implement this manually (without rkyv) so we have control over the [`Ord`] as interpreted by [`sled`].
    ///
    /// 9 bytes total per key, 1 for LOD and 8 for the morton code.
    fn as_sled_key(&self) -> Self::SledKey {
        let mut bytes = [0; 9];
        bytes[0] = self.level;
        bytes[1..].copy_from_slice(&self.morton.0.to_be_bytes());
        bytes
    }

    fn from_sled_key(bytes: &[u8]) -> Self {
        let level = bytes[0];
        let mut morton_bytes = [0; 8];
        morton_bytes.copy_from_slice(&bytes[1..]);
        let morton_int = u64::from_be_bytes(morton_bytes);
        Self::new(level, Morton2i32(morton_int))
    }

    fn extent_range(level: u8, extent: Extent<IVec2>) -> RangeInclusive<Self> {
        let min_morton = Morton2i32::from(extent.minimum);
        let max_morton = Morton2i32::from(extent.max());
        Self::new(level, min_morton)..=Self::new(level, max_morton)
    }

    fn min_key(level: u8) -> Self {
        Self::new(level, Morton2i32::from(IVec2::MIN))
    }

    fn max_key(level: u8) -> Self {
        Self::new(level, Morton2i32::from(IVec2::MAX))
    }
}

#[derive(
    Archive, Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize,
)]
#[archive_attr(derive(Debug, Eq, Hash, PartialEq, PartialOrd, Ord))]
pub struct DbKey3i32 {
    pub level: Level,
    pub morton: Morton3i32,
}

impl DbKey3i32 {
    pub fn new(level: Level, morton: Morton3i32) -> Self {
        Self { level, morton }
    }
}

impl DbKey for DbKey3i32 {
    type Coords = IVec3;
    type SledKey = [u8; 13];

    /// We implement this manually (without rkyv) so we have control over the [`Ord`] as interpreted by [`sled`].
    ///
    /// 13 bytes total per key, 1 for LOD and 12 for the morton code. Although a [`Morton3i32`] uses a u128, it only actually
    /// uses the least significant 96 bits (12 bytes).
    fn as_sled_key(&self) -> Self::SledKey {
        let mut bytes = [0; 13];
        bytes[0] = self.level;
        bytes[1..].copy_from_slice(&self.morton.0.to_be_bytes()[4..]);
        bytes
    }

    fn from_sled_key(bytes: &[u8]) -> Self {
        let level = bytes[0];
        // The most significant 4 bytes of the u128 are not used.
        let mut morton_bytes = [0; 16];
        morton_bytes[4..16].copy_from_slice(&bytes[1..]);
        let morton_int = u128::from_be_bytes(morton_bytes);
        Self::new(level, Morton3i32(morton_int))
    }

    fn extent_range(level: u8, extent: Extent<IVec3>) -> RangeInclusive<Self> {
        let min_morton = Morton3i32::from(extent.minimum);
        let max_morton = Morton3i32::from(extent.max());
        Self::new(level, min_morton)..=Self::new(level, max_morton)
    }

    fn min_key(level: u8) -> Self {
        Self::new(level, Morton3i32::from(IVec3::MIN))
    }

    fn max_key(level: u8) -> Self {
        Self::new(level, Morton3i32::from(IVec3::MAX))
    }
}
