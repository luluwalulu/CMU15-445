//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();

  auto catalog = exec_ctx_->GetCatalog();
  table_oid_t oid = plan_->GetTableOid();
  table_info_ = catalog->GetTable(oid);
}

// Update算子并非在原地内存上更新值，而是删除要被修改的那一行，然后插入被修改后的那一行
// Update算子必须避免的一种情况是一边取，一边做“删除插入”，因为Update算子会插入新的元组，导致会需要从下一层获取新的元组，导致死循环
// 所以需要先获取完所有元组，才一次性处理
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(is_finish_) {
    return false;
  }

  std::vector<Tuple*> tuples;
  Tuple child_tuple{};

  while(true){
    const auto status = child_executor_->Next(&child_tuple, rid);
    tuples.push_back(&child_tuple);
    if(!status) {
      break;
    }
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto table_heap = table_info_->table_.get();
  auto index_infos = catalog->GetTableIndexes(table_info_->name_);
  const auto& schema = table_info_->schema_;

  TupleMeta new_meta{0,false};
  TupleMeta delete_meta{0,true};

  int update_sum = 0;

  // 对于每个元组，将其设为deleted。然后计算得到新的元组，最后将新的元组插入，然后更新索引
  for(auto* t:tuples) {
    auto r = t->GetRid();
    auto old_meta = table_heap->GetTupleMeta(r);
    table_heap->UpdateTupleMeta(delete_meta,r);

    std::vector<Value> values;
    for(auto expr:plan_->target_expressions_) {
      values.push_back(expr->Evaluate(t, schema));
    }
    Tuple new_tuple(values, &schema);

    auto option_rid = table_heap->InsertTuple(new_meta, new_tuple);
    if(option_rid == std::nullopt) {
      continue;
    }

    // 插入成功
    new_tuple.SetRid(*option_rid);
    update_sum++;
    for(auto *info:index_infos){
      auto* index = info->index_.get();
      auto key_schema = index->GetKeySchema();
      const auto& key_attrs =index->GetKeyAttrs();

      auto key = child_tuple.KeyFromTuple(schema, *key_schema, key_attrs);
      info->index_->InsertEntry(key, *option_rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> return_values;
  return_values.emplace_back(TypeId::INTEGER, update_sum);
  *tuple = Tuple(return_values, &GetOutputSchema());
  rid->Set(INVALID_PAGE_ID, 0);

  is_finish_ = true;
  return true;
}

}  // namespace bustub
