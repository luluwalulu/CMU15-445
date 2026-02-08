//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  std::vector<Value> values;
  Tuple dummy_tuple;
  Value v;

  auto catalog = exec_ctx_->GetCatalog();
  auto index = catalog->GetIndex(plan_->index_oid_);
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index->index_.get());
  const auto &key_schema = index->key_schema_;

  if (plan_->pred_key_) {
    v = plan_->pred_key_->Evaluate(&dummy_tuple, Schema{{}});
  } else {
    const auto &expr = plan_->filter_predicate_;
    v = expr->Evaluate(&dummy_tuple, Schema{{}});
  }

  values.push_back(v);
  Tuple key(values, &key_schema);

  htable->ScanKey(key, &rids_, exec_ctx_->GetTransaction());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto table_heap = table_info->table_.get();

  while (cursor_ < rids_.size()) {
    auto p = table_heap->GetTuple(rids_[cursor_]);
    if (p.first.is_deleted_) {
      cursor_++;
      continue;
    }

    *tuple = p.second;
    *rid = rids_[cursor_];
    return true;
  }

  return false;
}

}  // namespace bustub
