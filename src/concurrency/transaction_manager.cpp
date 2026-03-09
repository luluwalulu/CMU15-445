//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_.load());

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto old_ts = last_commit_ts_.load();
  auto new_ts = old_ts + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // 接下来需要遍历该事务更改的所有元组，将时间戳设置为该提交时间戳
  for (const auto& pii : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(pii.first);
    auto table_heap = table_info->table_.get();
    
    for (auto& rid : pii.second) {
      TupleMeta meta = table_heap->GetTupleMeta(rid);
      meta.ts_ = new_ts;
      table_heap->UpdateTupleMeta(meta, rid);
    }
  }

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->state_ = TransactionState::COMMITTED;
  txn->commit_ts_.store(new_ts);
  
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  // 最终调整
  last_commit_ts_.store(new_ts);
  // std::cout<<"ID为"<<txn->GetTransactionId() - TXN_START_ID<<"的事务提交成功"<<std::endl;

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // std::unique_lock<std::shared_mutex> l(txn_map_mutex_);

  auto watermark = GetWatermark();
  std::unordered_map<timestamp_t, txn_id_t> tsToTxnid;
  std::cout<<"当前的水位线为"<<watermark<<std::endl;
  std::cout<<std::endl;

  for (auto p : txn_map_) {
    auto txn_id = p.first;
    auto txn = p.second;

    if (txn->GetTransactionState() == TransactionState::ABORTED || txn->GetTransactionState() == TransactionState::COMMITTED) {
      BUSTUB_ASSERT(txn->GetTransactionState() == TransactionState::COMMITTED, "狗屎ABORTED状态");
      tsToTxnid.emplace(txn->GetCommitTs(), txn_id);
      std::cout<<'t'<<txn_id - TXN_START_ID<<"的提交时间戳为"<<txn->GetCommitTs()<<std::endl;
    } else {
      std::cout<<"txn"<<txn_id - TXN_START_ID<<"的读时间戳为"<<txn->GetReadTs()<<std::endl;
    }
  }

  // 然后遍历所有表堆上的元素，并回溯直到对应UndoLog的提交时间戳小于水位线，此时可以将该事务Id排除在txn_ids之外
  for (const auto& name : catalog_->GetTableNames()) {
    auto table = catalog_->GetTable(name);
    auto table_heap = table->table_.get();
    auto itr = table_heap->MakeIterator();

    while (!itr.IsEnd()) {
      auto p = itr.GetTuple();
      auto base_meta = p.first;
      auto base_tuple = p.second;
      auto rid = base_tuple.GetRid();
      auto commit_ts = base_meta.ts_;
      auto undo_link = GetUndoLink(rid);
      UndoLink prev_link{};
      UndoLog undo_log{};
      BUSTUB_ASSERT(!(undo_link && !undo_link->IsValid()), "返回一个undo_link对象但是是无效连接");
      std::vector<timestamp_t> should_not_erased;

      // 回退
      while (commit_ts > watermark && undo_link->IsValid()) {
        undo_log = GetUndoLog(*undo_link);
        commit_ts = undo_log.ts_;
        prev_link = *undo_link;
        undo_link = undo_log.prev_version_;
        should_not_erased.push_back(commit_ts);
      }

      // 回退成功时，我们只知道对应事务的提交时间戳
      if (commit_ts <= watermark) {
        for (auto ts : should_not_erased) {
          tsToTxnid.erase(ts);
        }

        auto txn_id = tsToTxnid[commit_ts];
        auto txn = txn_map_[txn_id];
        undo_log.prev_version_ = {};
        txn->ModifyUndoLog(prev_link.prev_log_idx_, undo_log);
      }

      ++itr;
    }
  }

  for (const auto& p : tsToTxnid) {
    auto txn_id = p.second;
    txn_map_.erase(txn_id);
    std::cout<<'t'<<txn_id - TXN_START_ID<<"被删除"<<std::endl;
  }
}

}  // namespace bustub
