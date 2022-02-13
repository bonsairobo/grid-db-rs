//! A [`sled`](https://crates.io/crates/sled) database mapping from Morton-encoded (Z-order) quadtree/octree nodes to arbitrary `[u8]` data.

mod archived_buf;
mod backup_tree;
mod change_encoder;
mod db;
mod db_key;
mod meta_tree;
mod version_change_tree;
mod version_graph_tree;
mod working_tree;

pub use change_encoder::*;
pub use db::GridDb;
pub use db_key::*;
pub use meta_tree::GridDbMetadata;
pub use version_change_tree::VersionChanges;

use archived_buf::ArchivedBuf;

use ahash::AHashMap;
use rkyv::ser::serializers::{
    AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
};
use rkyv::{AlignedVec, Archive, Deserialize, Infallible, Serialize};
use sled::IVec;

pub use ilattice;
pub use rkyv;
pub use sled;

/// Level of detail.
pub type Level = u8;

/// Identifier of a particular archived [`VersionChanges`].
#[derive(
    Archive, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, PartialOrd, Ord, Serialize,
)]
#[archive_attr(derive(Debug, Eq, PartialEq, PartialOrd, Ord))]
pub struct Version {
    pub number: u64,
}

impl Version {
    pub const fn new(number: u64) -> Self {
        Self { number }
    }

    pub const fn into_sled_key(self) -> [u8; 8] {
        self.number.to_be_bytes()
    }
}

type SmallKeyHashMap<K, V> = AHashMap<K, V>;

type NoSharedAllocSerializer<const N: usize> = CompositeSerializer<
    AlignedSerializer<AlignedVec>,
    FallbackScratch<HeapScratch<N>, AllocScratch>,
    Infallible,
>;

type ArchivedIVec<T> = ArchivedBuf<T, IVec>;
