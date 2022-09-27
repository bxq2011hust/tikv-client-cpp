// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::{ops, path::PathBuf, time::Duration};

use anyhow::Result;
use cxx::{CxxString, CxxVector};
// use futures::executor::TOKIO_RUNTIME.block_on;
use once_cell::sync::{Lazy, OnceCell};
use slog::{o, Drain};
use std::fs::OpenOptions;
use std::sync::Once;
use tikv_client::{TimestampExt, TransactionOptions};
use tokio::runtime::Runtime;

use self::ffi::*;

static TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create TOKIO_RUNTIME")
});
static START: Once = Once::new();

#[cxx::bridge]
mod ffi {
    struct Key {
        key: Vec<u8>,
    }

    struct KvPair {
        key: Vec<u8>,
        value: Vec<u8>,
    }

    struct PrewriteResult {
        key: Vec<u8>,
        version: u64,
    }

    struct OptionalValue {
        is_none: bool,
        value: Vec<u8>,
    }

    enum Bound {
        Included,
        Excluded,
        Unbounded,
    }

    #[namespace = "tikv_client_glue"]
    extern "Rust" {
        type TransactionClient;
        type Transaction;
        type Snapshot;

        fn transaction_client_new(
            pd_endpoints: &CxxVector<CxxString>,
            logPath: &CxxString,
        ) -> Result<Box<TransactionClient>>;

        fn transaction_client_new_with_config(
            pd_endpoints: &CxxVector<CxxString>,
            log_path: &CxxString,
            ca_path: &CxxString,
            cert_path: &CxxString,
            key_path: &CxxString,
            timeout: u32,
        ) -> Result<Box<TransactionClient>>;

        fn transaction_client_begin(client: &TransactionClient) -> Result<Box<Transaction>>;

        fn transaction_client_begin_pessimistic(
            client: &TransactionClient,
        ) -> Result<Box<Transaction>>;

        fn transaction_get(transaction: &mut Transaction, key: &CxxString)
            -> Result<OptionalValue>;

        fn transaction_get_for_update(
            transaction: &mut Transaction,
            key: &CxxString,
        ) -> Result<OptionalValue>;

        fn transaction_batch_get(
            transaction: &mut Transaction,
            keys: &CxxVector<CxxString>,
        ) -> Result<Vec<KvPair>>;

        fn transaction_batch_get_for_update(
            transaction: &mut Transaction,
            keys: &CxxVector<CxxString>,
        ) -> Result<Vec<KvPair>>;

        fn transaction_scan(
            transaction: &mut Transaction,
            start: &CxxString,
            start_bound: Bound,
            end: &CxxString,
            end_bound: Bound,
            limit: u32,
        ) -> Result<Vec<KvPair>>;

        fn transaction_scan_keys(
            transaction: &mut Transaction,
            start: &CxxString,
            start_bound: Bound,
            end: &CxxString,
            end_bound: Bound,
            limit: u32,
        ) -> Result<Vec<Key>>;

        fn transaction_put(
            transaction: &mut Transaction,
            key: &CxxString,
            val: &CxxString,
        ) -> Result<()>;

        fn transaction_delete(transaction: &mut Transaction, key: &CxxString) -> Result<()>;

        fn transaction_commit(transaction: &mut Transaction) -> Result<()>;
        fn transaction_rollback(transaction: &mut Transaction) -> Result<()>;

        fn snapshot_new(client: &TransactionClient) -> Result<Box<Snapshot>>;

        fn snapshot_get(snapshot: &mut Snapshot, key: &CxxString) -> Result<OptionalValue>;

        fn snapshot_batch_get(
            snapshot: &mut Snapshot,
            keys: &CxxVector<CxxString>,
        ) -> Result<Vec<KvPair>>;

        fn snapshot_scan(
            snapshot: &mut Snapshot,
            start: &CxxString,
            start_bound: Bound,
            end: &CxxString,
            end_bound: Bound,
            limit: u32,
        ) -> Result<Vec<KvPair>>;

        fn snapshot_scan_keys(
            snapshot: &mut Snapshot,
            start: &CxxString,
            start_bound: Bound,
            end: &CxxString,
            end_bound: Bound,
            limit: u32,
        ) -> Result<Vec<Key>>;

        fn transaction_prewrite_primary(
            transaction: &mut Transaction,
            primary_key: &CxxString,
        ) -> Result<PrewriteResult>;

        fn transaction_prewrite_secondary(
            transaction: &mut Transaction,
            primary_key: &CxxString,
            start_ts: u64,
        ) -> Result<()>;
        fn transaction_commit_primary(transaction: &mut Transaction) -> Result<u64>;
        fn transaction_commit_secondary(transaction: &mut Transaction, commit_ts: u64);

    }
}

#[repr(transparent)]
struct TransactionClient {
    inner: tikv_client::TransactionClient,
}

#[repr(transparent)]
struct Transaction {
    inner: tikv_client::Transaction,
}

#[repr(transparent)]
struct Snapshot {
    inner: tikv_client::Snapshot,
}

fn create_slog_logger(log_path: &CxxString) -> Result<slog::Logger> {
    let mut log_path = log_path.to_str()?.to_string();
    log_path.push_str("/tikv-client.log");
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(log_path)
        .expect("open log file failed");

    let decorator = slog_term::PlainDecorator::new(file);
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());
    static SCOPE_GUARD: OnceCell<slog_scope::GlobalLoggerGuard> = OnceCell::new();
    #[allow(unused_must_use)]
    START.call_once(|| {
        SCOPE_GUARD
            .set(slog_scope::set_global_logger(logger));
        slog_stdlog::init().unwrap();
    });
    Ok(slog_scope::logger())
}
fn transaction_client_new(
    pd_endpoints: &CxxVector<CxxString>,
    log_path: &CxxString,
) -> Result<Box<TransactionClient>> {
    // env_logger::init();

    let log = create_slog_logger(log_path)?;
    let pd_endpoints = pd_endpoints
        .iter()
        .map(|str| str.to_str().map(ToOwned::to_owned))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(Box::new(TransactionClient {
        inner: TOKIO_RUNTIME
            .block_on(tikv_client::TransactionClient::new(pd_endpoints, Some(log)))?,
    }))
}

fn transaction_client_new_with_config(
    pd_endpoints: &CxxVector<CxxString>,
    log_path: &CxxString,
    ca_path: &CxxString,
    cert_path: &CxxString,
    key_path: &CxxString,
    timeout: u32,
) -> Result<Box<TransactionClient>> {
    let config = tikv_client::Config {
        ca_path: Some(PathBuf::from(ca_path.to_str()?.to_string())),
        cert_path: Some(PathBuf::from(cert_path.to_str()?.to_string())),
        key_path: Some(PathBuf::from(key_path.to_str()?.to_string())),
        timeout: Duration::from_secs(timeout as u64),
    };
    let log = create_slog_logger(log_path)?;
    let pd_endpoints = pd_endpoints
        .iter()
        .map(|str| str.to_str().map(ToOwned::to_owned))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(Box::new(TransactionClient {
        inner: TOKIO_RUNTIME.block_on(tikv_client::TransactionClient::new_with_config(
            pd_endpoints,
            config,
            Some(log),
        ))?,
    }))
}

fn transaction_client_begin(client: &TransactionClient) -> Result<Box<Transaction>> {
    Ok(Box::new(Transaction {
        inner: TOKIO_RUNTIME.block_on(client.inner.begin_optimistic())?,
    }))
}

fn transaction_client_begin_pessimistic(client: &TransactionClient) -> Result<Box<Transaction>> {
    Ok(Box::new(Transaction {
        inner: TOKIO_RUNTIME.block_on(client.inner.begin_pessimistic())?,
    }))
}

fn transaction_get(transaction: &mut Transaction, key: &CxxString) -> Result<OptionalValue> {
    match TOKIO_RUNTIME.block_on(transaction.inner.get(key.as_bytes().to_owned()))? {
        Some(value) => Ok(OptionalValue {
            is_none: false,
            value,
        }),
        None => Ok(OptionalValue {
            is_none: true,
            value: Vec::new(),
        }),
    }
}

fn transaction_get_for_update(
    transaction: &mut Transaction,
    key: &CxxString,
) -> Result<OptionalValue> {
    match TOKIO_RUNTIME.block_on(transaction.inner.get_for_update(key.as_bytes().to_owned()))? {
        Some(value) => Ok(OptionalValue {
            is_none: false,
            value,
        }),
        None => Ok(OptionalValue {
            is_none: true,
            value: Vec::new(),
        }),
    }
}

fn transaction_batch_get(
    transaction: &mut Transaction,
    keys: &CxxVector<CxxString>,
) -> Result<Vec<KvPair>> {
    let keys = keys.iter().map(|key| key.as_bytes().to_owned());
    let kv_pairs = TOKIO_RUNTIME
        .block_on(transaction.inner.batch_get(keys))?
        .map(|tikv_client::KvPair(key, value)| KvPair {
            key: key.into(),
            value,
        })
        .collect();
    Ok(kv_pairs)
}

fn transaction_batch_get_for_update(
    _transaction: &mut Transaction,
    _keys: &CxxVector<CxxString>,
) -> Result<Vec<KvPair>> {
    // let keys = keys.iter().map(|key| key.as_bytes().to_owned());
    // let kv_pairs = TOKIO_RUNTIME.block_on(transaction.inner.batch_get_for_update(keys))?
    //     .map(|tikv_client::KvPair(key, value)| KvPair {
    //         key: key.into(),
    //         value,
    //     })
    //     .collect();
    // Ok(kv_pairs)
    unimplemented!("batch_get_for_update is not working properly so far.")
}

fn transaction_scan(
    transaction: &mut Transaction,
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
    limit: u32,
) -> Result<Vec<KvPair>> {
    let range = to_bound_range(start, start_bound, end, end_bound);
    let kv_pairs = TOKIO_RUNTIME
        .block_on(transaction.inner.scan(range, limit))?
        .map(|tikv_client::KvPair(key, value)| KvPair {
            key: key.into(),
            value,
        })
        .collect();
    Ok(kv_pairs)
}

fn transaction_scan_keys(
    transaction: &mut Transaction,
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
    limit: u32,
) -> Result<Vec<Key>> {
    let range = to_bound_range(start, start_bound, end, end_bound);
    let keys = TOKIO_RUNTIME
        .block_on(transaction.inner.scan_keys(range, limit))?
        .map(|key| Key { key: key.into() })
        .collect();
    Ok(keys)
}

fn transaction_put(transaction: &mut Transaction, key: &CxxString, val: &CxxString) -> Result<()> {
    TOKIO_RUNTIME.block_on(
        transaction
            .inner
            .put(key.as_bytes().to_owned(), val.as_bytes().to_owned()),
    )?;
    Ok(())
}

fn transaction_delete(transaction: &mut Transaction, key: &CxxString) -> Result<()> {
    TOKIO_RUNTIME.block_on(transaction.inner.delete(key.as_bytes().to_owned()))?;
    Ok(())
}

fn transaction_commit(transaction: &mut Transaction) -> Result<()> {
    TOKIO_RUNTIME.block_on(transaction.inner.commit())?;
    Ok(())
}

fn transaction_rollback(transaction: &mut Transaction) -> Result<()> {
    TOKIO_RUNTIME.block_on(transaction.inner.rollback())?;
    Ok(())
}

fn transaction_prewrite_primary(
    transaction: &mut Transaction,
    primary_key: &CxxString,
) -> Result<PrewriteResult> {
    let primary_key = if primary_key.is_empty() {
        None
    } else {
        Some(primary_key.as_bytes().to_owned().into())
    };
    match TOKIO_RUNTIME.block_on(transaction.inner.prewrite_primary(primary_key)) {
        Ok((key, ts)) => Ok(PrewriteResult {
            key: key.into(),
            version: ts.version(),
        }),
        Err(e) => Err(e.into()),
    }
}

fn transaction_prewrite_secondary(
    transaction: &mut Transaction,
    primary_key: &CxxString,
    start_ts: u64,
) -> Result<()> {
    TOKIO_RUNTIME.block_on(transaction.inner.prewrite_secondary(
        primary_key.as_bytes().to_owned().into(),
        tikv_client::Timestamp::from_version(start_ts),
    ))?;
    Ok(())
}

fn transaction_commit_primary(transaction: &mut Transaction) -> Result<u64> {
    match TOKIO_RUNTIME.block_on(transaction.inner.commit_primary()) {
        Ok(ts) => Ok(ts.version()),
        Err(e) => Err(e.into()),
    }
}

fn transaction_commit_secondary(transaction: &mut Transaction, commit_ts: u64) {
    TOKIO_RUNTIME.block_on(
        transaction
            .inner
            .commit_secondary(tikv_client::Timestamp::from_version(commit_ts)),
    );
}

fn to_bound_range(
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
) -> tikv_client::BoundRange {
    let start_bound = match start_bound {
        Bound::Included => ops::Bound::Included(start.as_bytes().to_owned()),
        Bound::Excluded => ops::Bound::Excluded(start.as_bytes().to_owned()),
        Bound::Unbounded => ops::Bound::Unbounded,
        _ => panic!("unexpected bound"),
    };
    let end_bound = match end_bound {
        Bound::Included => ops::Bound::Included(end.as_bytes().to_owned()),
        Bound::Excluded => ops::Bound::Excluded(end.as_bytes().to_owned()),
        Bound::Unbounded => ops::Bound::Unbounded,
        _ => panic!("unexpected bound"),
    };
    tikv_client::BoundRange::from((start_bound, end_bound))
}

fn snapshot_new(client: &TransactionClient) -> Result<Box<Snapshot>> {
    let timestamp = TOKIO_RUNTIME.block_on(client.inner.current_timestamp())?;
    Ok(Box::new(Snapshot {
        inner: client
            .inner
            .snapshot(timestamp, TransactionOptions::new_optimistic()),
    }))
}

fn snapshot_get(snapshot: &mut Snapshot, key: &CxxString) -> Result<OptionalValue> {
    match TOKIO_RUNTIME.block_on(snapshot.inner.get(key.as_bytes().to_owned()))? {
        Some(value) => Ok(OptionalValue {
            is_none: false,
            value,
        }),
        None => Ok(OptionalValue {
            is_none: true,
            value: Vec::new(),
        }),
    }
}

fn snapshot_batch_get(snapshot: &mut Snapshot, keys: &CxxVector<CxxString>) -> Result<Vec<KvPair>> {
    let keys = keys.iter().map(|key| key.as_bytes().to_owned());
    let kv_pairs = TOKIO_RUNTIME
        .block_on(snapshot.inner.batch_get(keys))?
        .map(|tikv_client::KvPair(key, value)| KvPair {
            key: key.into(),
            value,
        })
        .collect();
    Ok(kv_pairs)
}

fn snapshot_scan(
    snapshot: &mut Snapshot,
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
    limit: u32,
) -> Result<Vec<KvPair>> {
    let range = to_bound_range(start, start_bound, end, end_bound);
    let kv_pairs = TOKIO_RUNTIME
        .block_on(snapshot.inner.scan(range, limit))?
        .map(|tikv_client::KvPair(key, value)| KvPair {
            key: key.into(),
            value,
        })
        .collect();
    Ok(kv_pairs)
}

fn snapshot_scan_keys(
    snapshot: &mut Snapshot,
    start: &CxxString,
    start_bound: Bound,
    end: &CxxString,
    end_bound: Bound,
    limit: u32,
) -> Result<Vec<Key>> {
    let range = to_bound_range(start, start_bound, end, end_bound);
    let keys = TOKIO_RUNTIME
        .block_on(snapshot.inner.scan_keys(range, limit))?
        .map(|key| Key { key: key.into() })
        .collect();
    Ok(keys)
}
