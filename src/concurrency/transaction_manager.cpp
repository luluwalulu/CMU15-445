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
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  auto watermark = GetWatermark();

  std::unordered_set<txn_id_t> protected_txn_ids;

  auto table_names = catalog_->GetTableNames();
  for (const auto &name : table_names) {
    auto table_info = catalog_->GetTable(name);
    auto table_heap = table_info->table_.get();
    auto itr = table_heap->MakeIterator();

    while (!itr.IsEnd()) {
      auto [base_meta, base_tuple] = itr.GetTuple();
      auto rid = base_tuple.GetRid();
      std::optional<UndoLink> curr_link = GetUndoLink(rid);

      // 如果一个连接都没有，什么都做不了
      if (!curr_link || !curr_link->IsValid()) {
        ++itr;
        continue;
      }

      timestamp_t version_ts = base_meta.ts_;
      UndoLink link = *curr_link;

      // 如果当前堆元组已经满足要求，之后的所有日志都被截断
      if (base_meta.ts_ <= watermark) {
        UpdateUndoLink(rid, std::nullopt);
        ++itr;
        continue;
      }

      // 如果当前元组尚未满足要求，需要往后遍历
      while (link.IsValid()) {
        // 通过link找到它连接的下一个log
        auto txn_id = link.prev_txn_;
        auto log_idx = link.prev_log_idx_;

        protected_txn_ids.insert(txn_id);

        auto txn = txn_map_[txn_id];
        UndoLog undo_log = txn->GetUndoLog(log_idx);

        version_ts = undo_log.ts_;

        // --- 核心逻辑：寻找截断点 ---
        // 如果该日志已经保留了最老有效快照，那么从version_ts开始（包括version_ts）的事务都可以删除
        if (version_ts <= watermark) {
          // 从这里开始截断
          if (undo_log.prev_version_.IsValid()) {
            undo_log.prev_version_ = UndoLink{};
            txn->ModifyUndoLog(log_idx, undo_log);
          }
          // 截断完成
          break;
        }

        link = undo_log.prev_version_;
      } // 寻找截断点
      ++itr;
    }
  }

  for (auto it = txn_map_.begin(); it != txn_map_.end(); ) {
    txn_id_t txn_id = it->first;
    auto txn = it->second;
    auto state = txn->GetTransactionState();

    if (state == TransactionState::COMMITTED && protected_txn_ids.find(txn_id) == protected_txn_ids.end()) {
      it = txn_map_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace bustub
