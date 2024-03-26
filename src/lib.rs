// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::ops;
// use std::{path::PathBuf};

use anyhow::Result;
use cxx::{CxxString, CxxVector};
// use futures::executor::block_on;
use log::debug;
use tokio::{
    runtime::Runtime,
    time::{timeout, Duration},
};
// use slog::{o, Drain};

use tikv_client::{request, Backoff, Config, Timestamp, TimestampExt, TransactionOptions};
use tokio::time::Instant;

use self::ffi::*;

// use once_cell::sync::Lazy;
// static TX_TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
//     tokio::runtime::Builder::new_multi_thread()
//         .worker_threads(4)
//         .enable_all()
//         .thread_name("transaction")
//         .build()
//         .expect("Failed to create TOKIO_RUNTIME")
// });
// static SNAPSHOT_TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
//     tokio::runtime::Builder::new_current_thread()
//         .worker_threads(4)
//         .enable_all()
//         .thread_name("snapshot")
//         .build()
//         .expect("Failed to create TOKIO_RUNTIME")
// });

#[cxx::bridge]
mod ffi {
    struct Key {
        key: Vec<u8>,
    }

    #[namespace = "ffi"]
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
        type RawKVClient;

        fn raw_client_new(pd_endpoints: &CxxVector<CxxString>) -> Result<Box<RawKVClient>>;

        fn raw_get(client: &RawKVClient, key: &CxxString, timeout_ms: u64)
            -> Result<OptionalValue>;

        fn raw_put(
            cli: &RawKVClient,
            key: &CxxString,
            val: &CxxString,
            timeout_ms: u64,
        ) -> Result<()>;

        fn raw_scan(
            cli: &RawKVClient,
            start: &CxxString,
            end: &CxxString,
            limit: u32,
            timeout_ms: u64,
        ) -> Result<Vec<KvPair>>;

        fn raw_delete(cli: &RawKVClient, key: &CxxString, timeout_ms: u64) -> Result<()>;

        fn raw_delete_range(
            cli: &RawKVClient,
            startKey: &CxxString,
            endKey: &CxxString,
            timeout_ms: u64,
        ) -> Result<()>;

        fn raw_batch_put(
            cli: &RawKVClient,
            pairs: &CxxVector<KvPair>,
            timeout_ms: u64,
        ) -> Result<()>;
        type Snapshot;

        fn transaction_client_new(
            pd_endpoints: &CxxVector<CxxString>,
            logPath: &CxxString,
            timeout: u32,
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
        fn client_gc(client: &TransactionClient, safeTimpoint: u64) -> Result<bool>;
        fn transaction_client_begin_optimistic_with_option(
            client: &TransactionClient,
            retry: u32,
        ) -> Result<Box<Transaction>>;

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
        fn current_timestamp(client: &TransactionClient) -> Result<u64>;

        fn snapshot_new(client: &TransactionClient, is_optimistic: bool) -> Result<Box<Snapshot>>;
        fn snapshot_new_with_timestamp(
            client: &TransactionClient,
            timestamp: u64,
        ) -> Result<Box<Snapshot>>;

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

// #[repr(transparent)]
struct TransactionClient {
    pub rt: tokio::runtime::Runtime,
    inner: tikv_client::TransactionClient,
}

struct RawKVClient {
    pub rt: tokio::runtime::Runtime,
    inner: tikv_client::RawClient,
}

struct Transaction {
    inner: tikv_client::Transaction,
    rt: tokio::runtime::Handle,
}

fn raw_client_new(pd_endpoints: &CxxVector<CxxString>) -> Result<Box<RawKVClient>> {
    // env_logger::builder()
    //     // .filter_level(log::LevelFilter::Info)
    //     .init();
    let runtime = Runtime::new().unwrap();
    let pd_endpoints = pd_endpoints
        .iter()
        .map(|str| str.to_str().map(ToOwned::to_owned))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(Box::new(RawKVClient {
        inner: runtime.block_on(tikv_client::RawClient::new(pd_endpoints))?,
        rt: runtime,
    }))
}

struct Snapshot {
    inner: tikv_client::Snapshot,
    rt: tokio::runtime::Handle,
}

use chrono;
// use once_cell::sync::OnceCell;
// use std::fs::OpenOptions;
use std::sync::Once;
static START: Once = Once::new();
// const DEFAULT_CHAN_SIZE: usize = 4096;
fn create_logger(log_path: &CxxString) -> Result<bool> {
    let mut log_path = log_path.to_str()?.to_string();

    let log_file_name = chrono::Local::now()
        .format("/tikv-client-%Y%m%d%H%M%S.log")
        .to_string();
    log_path.push_str(&log_file_name);
    // let file = OpenOptions::new()
    //     .create(true)
    //     .write(true)
    //     .truncate(false)
    //     .open(log_path)
    //     .expect("open log file failed");

    // let decorator = slog_term::PlainDecorator::new(file);
    // let drain = slog_term::FullFormat::new(decorator)
    //     .use_local_timestamp()
    //     .build()
    //     .fuse();
    // let drain = slog_async::Async::new(drain)
    //     .chan_size(DEFAULT_CHAN_SIZE)
    //     .build()
    //     .fuse();
    // let logger = slog::Logger::root(drain, o!());
    // static SCOPE_GUARD: OnceCell<slog_scope::GlobalLoggerGuard> = OnceCell::new();
    // #[allow(unused_must_use)]
    START.call_once(|| {
        // SCOPE_GUARD.set(slog_scope::set_global_logger(logger));
        // slog_stdlog::init().unwrap();
        env_logger::init();
    });
    // Ok(slog_scope::logger())
    Ok(true)
}

fn transaction_client_new(
    pd_endpoints: &CxxVector<CxxString>,
    log_path: &CxxString,
    timeout: u32,
) -> Result<Box<TransactionClient>> {
    let config = Config::default();
    let config = config.with_timeout(Duration::from_secs(timeout as u64));
    create_logger(log_path)?;
    let pd_endpoints = pd_endpoints
        .iter()
        .map(|str| str.to_str().map(ToOwned::to_owned))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let runtime = Runtime::new().unwrap();
    Ok(Box::new(TransactionClient {
        inner: runtime.block_on(tikv_client::TransactionClient::new_with_config(
            pd_endpoints,
            config,
        ))?,
        rt: runtime,
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
    let config = tikv_client::Config::default()
        .with_security(
            ca_path.to_str()?.to_string(),
            cert_path.to_str()?.to_string(),
            key_path.to_str()?.to_string(),
        )
        .with_timeout(Duration::from_secs(timeout as u64));
    // let config = tikv_client::Config {
    //     ca_path: Some(PathBuf::from(ca_path.to_str()?.to_string())),
    //     cert_path: Some(PathBuf::from(cert_path.to_str()?.to_string())),
    //     key_path: Some(PathBuf::from(key_path.to_str()?.to_string())),
    //     timeout: Duration::from_secs(timeout as u64),
    // };
    create_logger(log_path)?;
    let pd_endpoints = pd_endpoints
        .iter()
        .map(|str| str.to_str().map(ToOwned::to_owned))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let runtime = Runtime::new().unwrap();

    Ok(Box::new(TransactionClient {
        inner: runtime.block_on(tikv_client::TransactionClient::new_with_config(
            pd_endpoints,
            config,
        ))?,
        rt: runtime,
    }))
}

fn client_gc(client: &TransactionClient, safepoint: u64) -> Result<bool> {
    let safepoint = Timestamp::from_version(safepoint);
    Ok(client.rt.block_on(client.inner.gc(safepoint))?)
}

fn transaction_client_begin(client: &TransactionClient) -> Result<Box<Transaction>> {
    Ok(Box::new(Transaction {
        inner: client.rt.block_on(client.inner.begin_optimistic())?,
        rt: client.rt.handle().clone(),
    }))
}

fn transaction_client_begin_pessimistic(client: &TransactionClient) -> Result<Box<Transaction>> {
    Ok(Box::new(Transaction {
        inner: client.rt.block_on(client.inner.begin_pessimistic())?,
        rt: client.rt.handle().clone(),
    }))
}

fn raw_get(cli: &RawKVClient, key: &CxxString, timeout_ms: u64) -> Result<OptionalValue> {
    cli.rt.block_on(async {
        let result = timeout(
            Duration::from_millis(timeout_ms),
            cli.inner.get(key.as_bytes().to_vec()),
        )
        .await??;
        match result {
            Some(value) => Ok(OptionalValue {
                is_none: false,
                value,
            }),
            None => Ok(OptionalValue {
                is_none: true,
                value: Vec::new(),
            }),
        }
    })
}

fn raw_put(cli: &RawKVClient, key: &CxxString, val: &CxxString, timeout_ms: u64) -> Result<()> {
    cli.rt.block_on(async {
        timeout(
            Duration::from_millis(timeout_ms),
            cli.inner
                .put(key.as_bytes().to_vec(), val.as_bytes().to_vec()),
        )
        .await??;
        Ok(())
    })
}

fn raw_scan(
    cli: &RawKVClient,
    start: &CxxString,
    end: &CxxString,
    limit: u32,
    timeout_ms: u64,
) -> Result<Vec<KvPair>> {
    cli.rt.block_on(async {
        let rg = to_bound_range(start, Bound::Included, end, Bound::Excluded);
        let result =
            timeout(Duration::from_millis(timeout_ms), cli.inner.scan(rg, limit)).await??;
        let pairs = result
            .into_iter()
            .map(|tikv_client::KvPair(key, value)| KvPair {
                key: key.into(),
                value,
            })
            .collect();
        Ok(pairs)
    })
}

fn raw_delete(cli: &RawKVClient, key: &CxxString, timeout_ms: u64) -> Result<()> {
    cli.rt.block_on(async {
        timeout(
            Duration::from_millis(timeout_ms),
            cli.inner.delete(key.as_bytes().to_vec()),
        )
        .await??;
        Ok(())
    })
}

fn raw_delete_range(
    cli: &RawKVClient,
    start_key: &CxxString,
    end_key: &CxxString,
    timeout_ms: u64,
) -> Result<()> {
    cli.rt.block_on(async {
        let rg = to_bound_range(start_key, Bound::Included, end_key, Bound::Excluded);
        timeout(
            Duration::from_millis(timeout_ms),
            cli.inner.delete_range(rg),
        )
        .await??;
        Ok(())
    })
}

fn raw_batch_put(cli: &RawKVClient, pairs: &CxxVector<KvPair>, timeout_ms: u64) -> Result<()> {
    cli.rt.block_on(async {
        let tikv_pairs: Vec<tikv_client::KvPair> = pairs
            .iter()
            .map(|KvPair { key, value }| -> tikv_client::KvPair {
                tikv_client::KvPair(key.to_vec().into(), value.to_vec())
            })
            .collect();
        timeout(
            Duration::from_millis(timeout_ms),
            cli.inner.batch_put(tikv_pairs),
        )
        .await??;
        Ok(())
    })
}

fn transaction_client_begin_optimistic_with_option(
    client: &TransactionClient,
    retry: u32,
) -> Result<Box<Transaction>> {
    let options = TransactionOptions::new_optimistic();
    let mut retry_options = request::RetryOptions::default_optimistic();
    retry_options.lock_backoff = Backoff::no_jitter_backoff(2, 500, retry);
    let options = options.retry_options(retry_options);
    let timestamp = client.rt.block_on(client.inner.current_timestamp())?;
    Ok(Box::new(Transaction {
        inner: client
            .inner
            .new_transaction_with_options(timestamp, options),
        rt: client.rt.handle().clone(),
    }))
}

fn transaction_get(transaction: &mut Transaction, key: &CxxString) -> Result<OptionalValue> {
    match transaction
        .rt
        .block_on(transaction.inner.get(key.as_bytes().to_owned()))?
    {
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
    match transaction
        .rt
        .block_on(transaction.inner.get_for_update(key.as_bytes().to_owned()))?
    {
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
    let kv_pairs = transaction
        .rt
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
    // let kv_pairs = transaction.rt.block_on(transaction.inner.batch_get_for_update(keys))?
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
    let kv_pairs = transaction
        .rt
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
    let keys = transaction
        .rt
        .block_on(transaction.inner.scan_keys(range, limit))?
        .map(|key| Key { key: key.into() })
        .collect();
    Ok(keys)
}

fn transaction_put(transaction: &mut Transaction, key: &CxxString, val: &CxxString) -> Result<()> {
    transaction.rt.block_on(
        transaction
            .inner
            .put(key.as_bytes().to_owned(), val.as_bytes().to_owned()),
    )?;
    Ok(())
}

fn transaction_delete(transaction: &mut Transaction, key: &CxxString) -> Result<()> {
    transaction
        .rt
        .block_on(transaction.inner.delete(key.as_bytes().to_owned()))?;
    Ok(())
}

fn transaction_commit(transaction: &mut Transaction) -> Result<()> {
    transaction.rt.block_on(transaction.inner.commit())?;
    Ok(())
}

fn transaction_rollback(transaction: &mut Transaction) -> Result<()> {
    transaction.rt.block_on(transaction.inner.rollback())?;
    Ok(())
}

fn transaction_prewrite_primary(
    transaction: &mut Transaction,
    primary_key: &CxxString,
) -> Result<PrewriteResult> {
    let start = Instant::now();
    let primary_key = if primary_key.is_empty() {
        None
    } else {
        Some(primary_key.as_bytes().to_owned().into())
    };
    match transaction
        .rt
        .block_on(transaction.inner.prewrite_primary(primary_key))
    {
        Ok((key, ts)) => Ok({
            debug!("prewrite primary time {:?}", start.elapsed());
            PrewriteResult {
                key: key.into(),
                version: ts.version(),
            }
        }),
        Err(e) => {
            debug!("prewrite primary time {:?}", start.elapsed());
            Err(e.into())
        }
    }
}

fn transaction_prewrite_secondary(
    transaction: &mut Transaction,
    primary_key: &CxxString,
    start_ts: u64,
) -> Result<()> {
    let start = Instant::now();
    transaction
        .rt
        .block_on(transaction.inner.prewrite_secondary(
            primary_key.as_bytes().to_owned().into(),
            tikv_client::Timestamp::from_version(start_ts),
        ))?;
    debug!("prewrite secondary time {:?}", start.elapsed());
    Ok(())
}

fn transaction_commit_primary(transaction: &mut Transaction) -> Result<u64> {
    let start = Instant::now();
    match transaction.rt.block_on(transaction.inner.commit_primary()) {
        Ok(ts) => {
            debug!("commit primary time {:?}", start.elapsed());
            Ok(ts.version())
        }
        Err(e) => Err(e.into()),
    }
}

fn transaction_commit_secondary(transaction: &mut Transaction, commit_ts: u64) {
    let start: Instant = Instant::now();
    transaction.rt.block_on(
        transaction
            .inner
            .commit_secondary(tikv_client::Timestamp::from_version(commit_ts)),
    );
    debug!("commit secondary time {:?}", start.elapsed());
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

fn snapshot_new(client: &TransactionClient, is_optimistic: bool) -> Result<Box<Snapshot>> {
    let timestamp = client.rt.block_on(client.inner.current_timestamp())?;
    Ok(Box::new(Snapshot {
        inner: client.inner.snapshot(
            timestamp,
            match is_optimistic {
                true => TransactionOptions::new_optimistic(),
                false => TransactionOptions::new_pessimistic(),
            },
        ),
        rt: client.rt.handle().clone(),
    }))
}

fn snapshot_new_with_timestamp(
    client: &TransactionClient,
    timestamp: u64,
) -> Result<Box<Snapshot>> {
    let timestamp = tikv_client::Timestamp::from_version(timestamp);
    Ok(Box::new(Snapshot {
        inner: client
            .inner
            .snapshot(timestamp, TransactionOptions::new_optimistic()),
        rt: client.rt.handle().clone(),
    }))
}

fn current_timestamp(client: &TransactionClient) -> Result<u64> {
    let timestamp = client.rt.block_on(client.inner.current_timestamp())?;
    Ok(timestamp.version())
}

fn snapshot_get(snapshot: &mut Snapshot, key: &CxxString) -> Result<OptionalValue> {
    match snapshot
        .rt
        .block_on(snapshot.inner.get(key.as_bytes().to_owned()))?
    {
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
    let kv_pairs = snapshot
        .rt
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
    let kv_pairs = snapshot
        .rt
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
    let keys = snapshot
        .rt
        .block_on(snapshot.inner.scan_keys(range, limit))?
        .map(|key| Key { key: key.into() })
        .collect();
    Ok(keys)
}
