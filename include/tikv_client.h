// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#ifndef _TIKV_CLIENT_H_
#define _TIKV_CLIENT_H_

#include "tikv_client_glue.h"
#include <iostream>
#include <map>
#include <memory>
#include <optional>

namespace tikv_client {

struct KvPair final {
  std::string key;
  std::string value;

  KvPair(std::string &&key, std::string &&value);
};

class Transaction {
public:
  Transaction(::rust::cxxbridge1::Box<tikv_client_glue::Transaction> txn);
  std::optional<std::string> get(const std::string &key);
  std::optional<std::string> get_for_update(const std::string &key);
  std::vector<KvPair> batch_get(const std::vector<std::string> &keys);
  std::vector<KvPair>
  batch_get_for_update(const std::vector<std::string> &keys);
  std::vector<KvPair> scan(const std::string &start, Bound start_bound,
                           const std::string &end, Bound end_bound,
                           std::uint32_t limit);
  std::vector<std::string> scan_keys(const std::string &start,
                                     Bound start_bound, const std::string &end,
                                     Bound end_bound, std::uint32_t limit);
  void put(const std::string &key, const std::string &value);
  void batch_put(const std::vector<KvPair> &kvs);
  void remove(const std::string &key);
  void commit();
  void rollback();
  std::pair<std::string, uint64_t>
  prewrite_primary(const std::string &primary_key);
  void prewrite_secondary(const std::string &primary_key, uint64_t start_ts);
  uint64_t commit_primary();
  void commit_secondary(uint64_t commit_ts);

private:
  ::rust::cxxbridge1::Box<tikv_client_glue::Transaction> _txn;
};

class Snapshot {
public:
  Snapshot(::rust::cxxbridge1::Box<tikv_client_glue::Snapshot> snapshot);
  std::optional<std::string> get(const std::string &key);
  std::map<std::string, std::string>
  batch_get(const std::vector<std::string> &keys);
  std::vector<KvPair> scan(const std::string &start, Bound start_bound,
                           const std::string &end, Bound end_bound,
                           std::uint32_t limit);
  std::vector<std::string> scan_keys(const std::string &start,
                                     Bound start_bound, const std::string &end,
                                     Bound end_bound, std::uint32_t limit);

private:
  ::rust::cxxbridge1::Box<tikv_client_glue::Snapshot> _snapshot;
};

class TransactionClient {
public:
  TransactionClient(const std::vector<std::string> &pd_endpoints,
                    const std::string &log_path, uint32_t grpc_timeout = 3);
  TransactionClient(const std::vector<std::string> &pd_endpoints,
                    const std::string &log_path, const std::string &ca_path,
                    const std::string &cert_path, const std::string &key_path,
                    uint32_t timeout = 3);
  Transaction begin();
  std::shared_ptr<Transaction> new_optimistic_transaction();
  std::shared_ptr<Transaction> new_optimistic_transaction(uint32_t retry_limit);
  Transaction begin_pessimistic();
  std::shared_ptr<Snapshot> snapshot();
  std::shared_ptr<Snapshot> snapshot(uint64_t timestamp);
  uint64_t current_timestamp();
  void gc(uint64_t safe_point);

private:
  ::rust::cxxbridge1::Box<tikv_client_glue::TransactionClient> _client;
};

} // namespace tikv_client

#endif //_TIKV_CLIENT_H_
