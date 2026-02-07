//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(is_finished) {
    return false;
  }

  Tuple child_tuple{};
  TupleMeta delete_meta{0,true};
  int delete_sum = 0;

  auto catalog = exec_ctx_->GetCatalog();
  table_oid_t oid = plan_->GetTableOid();
  auto table_info = catalog->GetTable(oid);
  auto table_heap = table_info->table_.get();

  auto index_info = catalog->GetTableIndexes(table_info->name_);

  const auto& schema = table_info->schema_;

  while(true) {
    const auto status = child_executor_->Next(&child_tuple, rid);
    if(!status) {
      break;
    }

    // 删除对应元组
    auto r = child_tuple.GetRid();
    table_heap->UpdateTupleMeta(delete_meta, r);

    delete_sum++;
    for(auto *info:index_info) {
      auto* index = info->index_.get();
      auto key_schema = index->GetKeySchema();
      const auto& key_attrs =index->GetKeyAttrs();

      auto key = child_tuple.KeyFromTuple(schema, *key_schema, key_attrs);
      info->index_->DeleteEntry(key, r, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> return_values;
  return_values.emplace_back(TypeId::INTEGER, delete_sum);
  *tuple = Tuple(return_values, &GetOutputSchema());
  rid->Set(INVALID_PAGE_ID, 0);

  is_finished = true;
  return true;
}

}  // namespace bustub
