use super::{db::AbortReason, ArchivedChangeIVec, DbKey, EncodedChanges, VersionChanges};

use sled::transaction::{
    ConflictableTransactionError, TransactionalTree, UnabortableTransactionError,
};
use sled::Tree;
use std::collections::{BTreeMap, BTreeSet};

pub fn open_backup_tree<K>(map_name: &str, db: &sled::Db) -> sled::Result<(Tree, BackupKeyCache<K>)>
where
    K: DbKey,
{
    let tree = db.open_tree(format!("{}-backup", map_name))?;
    let mut keys = BTreeSet::default();
    for iter_result in tree.iter() {
        let (key_bytes, _) = iter_result?;
        keys.insert(K::from_sled_key(&key_bytes));
    }
    Ok((tree, BackupKeyCache { keys }))
}

pub fn write_changes_to_backup_tree(
    txn: &TransactionalTree,
    changes: EncodedChanges,
) -> Result<(), UnabortableTransactionError> {
    for (key_bytes, change) in changes.changes.into_iter() {
        txn.insert(&key_bytes, change.take_bytes())?;
    }
    Ok(())
}

pub fn commit_backup<K>(
    txn: &TransactionalTree,
    keys: &BackupKeyCache<K>,
) -> Result<VersionChanges<K>, ConflictableTransactionError<AbortReason>>
where
    K: DbKey,
{
    let mut changes = BTreeMap::default();
    for key in keys.keys.iter() {
        if let Some(change) = txn.remove(key.as_sled_key().as_ref())? {
            let archived_change = unsafe { ArchivedChangeIVec::new(change) };
            changes.insert(key.clone(), archived_change.deserialize());
        } else {
            panic!("BUG: failed to get change backup for {:?}", key);
        }
    }
    Ok(VersionChanges::new(changes))
}

pub fn clear_backup<K>(
    txn: &TransactionalTree,
    keys: &BackupKeyCache<K>,
) -> Result<(), UnabortableTransactionError>
where
    K: DbKey,
{
    for key in keys.keys.iter() {
        txn.remove(key.as_sled_key().as_ref())?;
    }
    Ok(())
}

/// The set of keys currently stored in the backup tree. Equivalently: the set of keys that have been changed from the parent
/// version to the working version.
#[derive(Clone, Default)]
pub struct BackupKeyCache<K> {
    /// [`BTreeSet`] is used for sorted iteration; which implies linear traversal over a sled tree.
    pub keys: BTreeSet<K>,
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
    use crate::{Change, ChangeEncoder, DbKey3i32};

    use ilattice::glam::IVec3;
    use sled::transaction::TransactionError;

    #[test]
    fn write_and_commit_backup() {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let (tree, mut backup_keys) = open_backup_tree("mymap", &db).unwrap();

        assert!(backup_keys.keys.is_empty());

        let key1 = DbKey3i32::new(1, IVec3::ZERO.into());
        let key2 = DbKey3i32::new(2, IVec3::ONE.into());
        backup_keys.keys.insert(key1);
        backup_keys.keys.insert(key2);

        let mut encoder = ChangeEncoder::default();
        encoder.add_change(key1, Change::Remove);
        encoder.add_change(key2, Change::Insert(Box::new([0])));
        let encoded_changes = encoder.encode();

        let _: Result<_, TransactionError<AbortReason>> = tree.transaction(|txn| {
            write_changes_to_backup_tree(txn, encoded_changes.clone())?;
            let reverse_changes = commit_backup(txn, &backup_keys)?;
            assert_eq!(
                reverse_changes.changes,
                BTreeMap::from([
                    (key1, Change::Remove),
                    (key2, Change::Insert(Box::new([0])))
                ])
            );
            Ok(())
        });
    }
}
