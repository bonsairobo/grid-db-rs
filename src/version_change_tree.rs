use super::{ArchivedIVec, Change, DbKey, EncodedChanges, Version};
use crate::NoSharedAllocSerializer;

use rkyv::ser::Serializer;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use sled::transaction::TransactionalTree;
use sled::{transaction::UnabortableTransactionError, Tree};
use std::collections::BTreeMap;

#[derive(Archive, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VersionChanges<K> {
    /// The full set of changes made between `parent_version` and this version.
    ///
    /// Kept in a btree map to be efficiently searchable by readers of the archive.
    pub changes: BTreeMap<K, Change>,
}

impl<K> VersionChanges<K> {
    pub fn new(changes: BTreeMap<K, Change>) -> Self {
        Self { changes }
    }
}

impl<K> From<&EncodedChanges> for VersionChanges<K>
where
    K: DbKey,
{
    fn from(changes: &EncodedChanges) -> Self {
        Self {
            changes: BTreeMap::from_iter(
                changes
                    .changes
                    .iter()
                    .map(|(key, value)| (K::from_sled_key(key), value.deserialize())),
            ),
        }
    }
}

pub fn open_version_change_tree(map_name: &str, db: &sled::Db) -> sled::Result<Tree> {
    db.open_tree(format!("{}-version-changes", map_name))
}

pub fn archive_version<K>(
    txn: &TransactionalTree,
    version: Version,
    changes: &VersionChanges<K>,
) -> Result<(), UnabortableTransactionError>
where
    K: DbKey,
    Archived<K>: Ord,
{
    let mut serializer = NoSharedAllocSerializer::<8192>::default();
    serializer.serialize_value(changes).unwrap();
    let changes_bytes = serializer.into_serializer().into_inner();
    txn.insert(&version.into_sled_key(), changes_bytes.as_ref())?;
    Ok(())
}

pub fn remove_archived_version<K>(
    txn: &TransactionalTree,
    version: Version,
) -> Result<Option<ArchivedIVec<VersionChanges<K>>>, UnabortableTransactionError>
where
    VersionChanges<K>: Archive,
{
    let bytes = txn.remove(&version.into_sled_key())?;
    Ok(bytes.map(|b| unsafe { ArchivedIVec::<VersionChanges<K>>::new(b) }))
}

// ████████╗███████╗███████╗████████╗
// ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝
//    ██║   █████╗  ███████╗   ██║
//    ██║   ██╔══╝  ╚════██║   ██║
//    ██║   ███████╗███████║   ██║
//    ╚═╝   ╚══════╝╚══════╝   ╚═╝

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbKey3i32;

    use ilattice::glam::IVec3;
    use rkyv::option::ArchivedOption;

    use sled::transaction::TransactionError;

    #[derive(Archive, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct Value(u32);

    #[test]
    fn open_archive_and_get() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let tree = db.open_tree("mymap-changes").unwrap();
        let v0 = Version::new(0);

        let mut original_changes = BTreeMap::new();
        original_changes.insert(
            DbKey3i32::new(1, IVec3::ZERO.into()),
            Change::Insert(Box::new([0])),
        );
        original_changes.insert(DbKey3i32::new(2, IVec3::ZERO.into()), Change::Remove);
        let changes = VersionChanges::new(original_changes.clone());

        let changes: Result<VersionChanges<DbKey3i32>, TransactionError> =
            tree.transaction(|txn| {
                assert!(
                    remove_archived_version(txn, v0).unwrap()
                        == ArchivedOption::<ArchivedIVec<VersionChanges<DbKey3i32>>>::None
                );

                archive_version(txn, v0, &changes).unwrap();

                let owned_archive = remove_archived_version(txn, Version::new(0))?.unwrap();

                Ok(owned_archive.deserialize())
            });
        assert_eq!(changes.unwrap(), VersionChanges::new(original_changes));
    }
}
