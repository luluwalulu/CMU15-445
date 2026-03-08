//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  txn_manager_ = exec_ctx_->GetTransactionManager();
  txn_ = exec_ctx_->GetTransaction();
  auto catalog = exec_ctx_->GetCatalog();
  table_oid_t oid = plan_->GetTableOid();
  auto table_info = catalog->GetTable(oid);
  itr_.emplace(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!itr_.has_value()) {
    std::cout << "在执行SeqScanExecutor::Next之前本应进行初始化" << std::endl;
    return false;
  }

  while (!itr_->IsEnd()) {
    std::cout<<"while循环开始"<<std::endl;
    auto pii = itr_->GetTuple();

    auto base_meta = pii.first;
    auto base_tuple = pii.second;

    // std::cout<<"seq_scan_executor中base_tuple的RID为"<<base_tuple.GetRid().ToString();
    Tuple new_tuple{base_tuple};
    // 表示堆中元组的提交时间戳
    auto heap_ts = base_meta.ts_;
    // 表示该事务的读时间戳
    auto read_ts = txn_->GetReadTs();
    std::cout<<"while循环开始2"<<std::endl;
    // 表示版本链中的第一个UndoLink连接
    auto undo_link = txn_manager_->GetUndoLink(base_tuple.GetRid());
    std::cout<<"while循环开始3"<<std::endl;
    // 存储回退版本
    std::vector<UndoLog> undo_logs;
    // 表示当前版本的元组是否被删除
    bool is_deleted{base_meta.is_deleted_};
    // 获取当前事务的ID
    auto transac_id = txn_->GetTransactionId();

    // 如果堆中元组处于最新状态可能需要回退
    // 堆中元组处于修改状态且该修改不来自当前事务同样需要回退
    // 只有该元组处于修改状态且该修改来自当前事务时才不需要回退
    if (heap_ts < TXN_START_ID || heap_ts != transac_id) {
      std::cout<<"该元组经历回退"<<std::endl;
      while (read_ts < heap_ts && undo_link.has_value() && undo_link->IsValid()) {
        auto undo_log = txn_manager_->GetUndoLog(*undo_link);
        undo_logs.push_back(undo_log);
        heap_ts = undo_log.ts_;
        undo_link = undo_log.prev_version_;
        is_deleted = undo_log.is_deleted_;
      }

      if (read_ts >= heap_ts && !is_deleted) {
        // 回退到了正确的版本且没有被删除 或者 修改来自当前事务且元组未被删除
        std::cout<<"该元组回退通过"<<std::endl;
        new_tuple = *ReconstructTuple(&GetOutputSchema(), base_tuple, base_meta, undo_logs);
      } else {
        std::cout<<"该元组回退到最后都不满足条件"<<std::endl;
        ++*itr_;
        continue;
      }
    }

    if (is_deleted) {
      // std::cout<<"堆上元组被当前事务修改，但是处于被删除状态"<<std::endl;
      ++*itr_;
      continue;
    }

    if (plan_->filter_predicate_) {
      auto v = plan_->filter_predicate_->Evaluate(&new_tuple, GetOutputSchema());
      BUSTUB_ASSERT(!v.IsNull(), "v不能为空");
      if (!v.GetAs<bool>()) {
        ++*itr_;
        continue;
      }
    }

    *tuple = new_tuple;
    *rid = new_tuple.GetRid();
    std::cout<<"回退后该元组的rid为"<<rid->ToString();
    ++*itr_;
    return true;
  }

  return false;
}

}  // namespace bustub
