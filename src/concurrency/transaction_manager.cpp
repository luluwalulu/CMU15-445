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
  // std::unique_lock<std::shared_mutex> l(txn_map_mutex_);

  auto watermark = GetWatermark();
  std::unordered_map<timestamp_t, txn_id_t> tsToTxnid;
  std::unordered_set<timestamp_t> should_not_erased_ts;
  std::cout<<"当前的水位线为"<<watermark<<std::endl;
  std::cout<<std::endl;

  std::cout<<"执行垃圾回收之前的事务有:"<<std::endl;
  for (auto p : txn_map_) {
    auto txn_id = p.first;
    auto txn = p.second;

    if (txn->GetTransactionState() == TransactionState::ABORTED || txn->GetTransactionState() == TransactionState::COMMITTED) {
      BUSTUB_ASSERT(txn->GetTransactionState() == TransactionState::COMMITTED, "狗屎ABORTED状态");
      tsToTxnid.emplace(txn->GetCommitTs(), txn_id);
      std::cout<<'t'<<ReadableTxnID(txn_id)<<"的提交时间戳为"<<txn->GetCommitTs()<<std::endl;
    } else {
      std::cout<<"txn"<<ReadableTxnID(txn_id)<<"的读时间戳为"<<txn->GetReadTs()<<std::endl;
    }
  }
  std::cout<<std::endl;

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
      auto undo_link = GetUndoLink(rid);
      std::cout<<"RID"<<rid.GetPageId()<<'/'<<rid.GetSlotNum()<<std::endl;

      auto heap_ts = base_meta.ts_;
      auto commit_ts = base_meta.ts_;
      UndoLink prev_link{};
      UndoLog undo_log{};
      BUSTUB_ASSERT(!(undo_link && !undo_link->IsValid()), "返回一个undo_link对象但是是无效连接");

      // 假设一个元组应该保存的日志组成一条版本链，最后一个日志的时间戳对应的事务不需要保存
      while (commit_ts > watermark && undo_link && undo_link->IsValid()) {
        if (commit_ts < TXN_START_ID) {
          should_not_erased_ts.emplace(commit_ts);
        }
        std::cout<<"commit_ts = "<<commit_ts<<std::endl;
        undo_log = GetUndoLog(*undo_link);
        auto temp = commit_ts;
        commit_ts = undo_log.ts_;
        prev_link = *undo_link;
        undo_link = undo_log.prev_version_;

        if (undo_log.ts_ <= watermark || !undo_link->IsValid()) {
          commit_ts = temp;
          std::cout<<"commit_ts被回退到"<<temp<<std::endl;
          break;
        }
        // auto txn_id = tsToTxnid[]
      }

      std::cout<<"最终commit_ts为"<<commit_ts<<std::endl;
      BUSTUB_ASSERT(tsToTxnid.find(commit_ts) != tsToTxnid.end(), "tsToTxnid中一定存在commit_ts");
      if (undo_link->IsValid()) {
        auto txn_id = tsToTxnid[commit_ts];
        // std::cout<<"最后一个版本的txn_id为"<<ReadableTxnID(txn_id)<<std::endl;
        BUSTUB_ASSERT(txn_map_.find(txn_id) != txn_map_.end(), "txn_map中一定能找到对应的txn_id");
        auto txn = txn_map_[txn_id];
        undo_log.prev_version_ = {};
        // std::cout<<"txn的撤销日志总数量为"<<txn->GetUndoLogNum()<<std::endl;
        // std::cout<<"需要修改的log_idx为"<<prev_link.prev_log_idx_<<std::endl;
        txn->ModifyUndoLog(prev_link.prev_log_idx_, undo_log);
      }

      ++itr;
    }
  }

  // 进行实际的删除操作
  for (const auto& p : tsToTxnid) {
    auto commit_ts = p.first;
    auto txn_id = p.second;
    if (should_not_erased_ts.find(commit_ts) == should_not_erased_ts.end()) {
      txn_map_.erase(txn_id);
      std::cout<<'t'<<ReadableTxnID(txn_id)<<"被删除"<<std::endl;
    }
  }
}

}  // namespace bustub
