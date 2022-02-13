use super::{ArchivedIVec, DbKey};
use crate::{NoSharedAllocSerializer, SmallKeyHashMap};
use rkyv::{
    ser::{serializers::CoreSerializer, Serializer},
    AlignedBytes, AlignedVec, Archive, Archived, Deserialize, Serialize,
};

use sled::IVec;

#[derive(Archive, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Change {
    Insert(Box<[u8]>),
    Remove,
}

impl Change {
    pub fn unwrap_insert(self) -> Box<[u8]> {
        match self {
            Change::Insert(x) => x,
            Change::Remove => panic!("Unwrapped on Change::Remove"),
        }
    }

    pub fn map(self, mut f: impl FnMut(Box<[u8]>) -> Box<[u8]>) -> Change {
        match self {
            Change::Insert(x) => Change::Insert(f(x)),
            Change::Remove => Change::Remove,
        }
    }
}

impl Change {
    pub fn serialize(&self) -> AlignedVec {
        let mut serializer = NoSharedAllocSerializer::<8912>::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner()
    }

    pub fn serialize_remove<const N: usize>() -> AlignedBytes<N>
    where
        Change: Serialize<CoreSerializer<N, 0>>,
    {
        let mut serializer = CoreSerializer::<N, 0>::default();
        serializer.serialize_value(&Change::Remove).unwrap();
        serializer.into_serializer().into_inner()
    }
}

impl ArchivedChange {
    pub fn get_insert_data(&self) -> Option<&Archived<Box<[u8]>>> {
        match self {
            Self::Insert(data) => Some(data),
            Self::Remove => None,
        }
    }
}

/// Creates an [`EncodedChanges`].
///
/// Prevents duplicates, keeping the latest change. Also sorts the changes by Morton order for efficient DB insertion.
pub struct ChangeEncoder<K> {
    added_changes: SmallKeyHashMap<K, Change>,
}

impl<K> Default for ChangeEncoder<K> {
    fn default() -> Self {
        Self {
            added_changes: Default::default(),
        }
    }
}

impl<K> ChangeEncoder<K>
where
    K: DbKey,
{
    pub fn add_change(&mut self, key: K, change: Change) {
        self.added_changes.insert(key, change);
    }

    /// Sorts the changes by Morton key and converts them to `IVec` key-value pairs for `sled`.
    pub fn encode(self) -> EncodedChanges {
        // Serialize values.
        let mut changes: Vec<_> = self
            .added_changes
            .into_iter()
            .map(|(key, change)| {
                (key, unsafe {
                    // PERF: sad that we can't serialize directly into an IVec
                    ArchivedIVec::new(IVec::from(change.serialize().as_ref()))
                })
            })
            .collect();

        // Sort by the ord key.
        changes.sort_by_key(|(key, _change)| key.clone());

        // Serialize the keys.
        let changes: Vec<_> = changes
            .into_iter()
            .map(|(key, change)| (IVec::from(key.as_sled_key().as_ref()), change))
            .collect();

        EncodedChanges { changes }
    }
}

/// A set of [Change]s to be atomically applied to a [`GridDb`](crate::GridDb).
///
/// Should be created with a [`ChangeEncoder`], which is guaranteed to drop duplicate changes on the same key, keeping only the
/// latest changes.
#[derive(Clone, Debug, Default)]
pub struct EncodedChanges {
    pub changes: Vec<(IVec, ArchivedChangeIVec)>,
}

/// We use this format for all changes stored in the working tree and backup tree.
///
/// Any values written to the working tree must be [`Change::Insert`] variants, but [`Change::Remove`]s are allowed and
/// necessary inside the backup tree.
///
/// By using the same format for values in both trees, we don't need to re-serialize them when moving any entry from the working
/// tree to the backup tree.
pub type ArchivedChangeIVec = ArchivedIVec<Change>;

// ████████╗███████╗███████╗████████╗
// ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝
//    ██║   █████╗  ███████╗   ██║
//    ██║   ██╔══╝  ╚════██║   ██║
//    ██║   ███████╗███████║   ██║
//    ╚═╝   ╚══════╝╚══════╝   ╚═╝

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archived_buf::ArchivedBuf;

    use sled::IVec;

    #[test]
    fn deserialize_remove_bytes() {
        // This needs to be 12! Leaving empty space at the end of the AlignedBytes will cause archive_root to fail.
        let remove_bytes: ArchivedBuf<Change, AlignedBytes<12>> =
            unsafe { ArchivedBuf::new(Change::serialize_remove::<12>()) };
        assert_eq!(remove_bytes.deserialize(), Change::Remove);
    }

    #[test]
    fn deserialize_insert_bytes() {
        let original = Change::Insert(Box::new([0]));
        let serialized =
            unsafe { ArchivedIVec::<Change>::new(IVec::from(original.serialize().as_ref())) };
        let deserialized = serialized.deserialize();
        assert_eq!(deserialized, original);
    }
}
