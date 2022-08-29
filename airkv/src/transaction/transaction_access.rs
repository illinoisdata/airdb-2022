use crate::{io::storage_connector::StorageConnector, storage::{data_entry::AppendRes, segment::SegSize}};

use super::transaction::Transaction;

pub trait TransactionAccess {
    fn append_transaction(
        &mut self,
        conn: &dyn StorageConnector,
        txn: &dyn Transaction,
    ) -> AppendRes<SegSize>;

    // fn varify_transactiion(
    //     &mut self,
    //     conn: &dyn StorageConnector,
    //     txn: &dyn Transaction
    // ) -> GResult<bool>;
}
