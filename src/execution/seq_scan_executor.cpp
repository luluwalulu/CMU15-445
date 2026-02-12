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
  auto catalog = exec_ctx_->GetCatalog();
  table_oid_t oid = plan_->GetTableOid();
  auto table_info = catalog->GetTable(oid);
  itr_.emplace(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 不需要从child_executor那里获取什么，直接根据itr_得到下一个应该返回的tuple
  if (!itr_.has_value()) {
    std::cout << "在执行SeqScanExecutor::Next之前本应进行初始化" << std::endl;
    return false;
  }

  while (!itr_->IsEnd()) {
    auto pii = itr_->GetTuple();
    if (pii.first.is_deleted_) {
      ++*itr_;
      continue;
    }
    if (plan_->filter_predicate_) {
      auto v = plan_->filter_predicate_->Evaluate(&pii.second, GetOutputSchema());
      BUSTUB_ASSERT(!v.IsNull(), "v不能为空");
      if (!v.GetAs<bool>()) {
        ++*itr_;
        continue;
      }
    }
    *tuple = pii.second;
    *rid = itr_->GetRID();
    tuple->SetRid(*rid);
    ++*itr_;
    return true;
  }

  return false;
}

}  // namespace bustub
