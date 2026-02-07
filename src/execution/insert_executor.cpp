//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {  }

void InsertExecutor::Init() {
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(is_finish_) {
    return false;
  }

  Tuple child_tuple{};

  auto catalog = exec_ctx_->GetCatalog();
  table_oid_t oid = plan_->GetTableOid();
  auto table_info = catalog->GetTable(oid);
  auto table_heap = table_info->table_.get();

  auto index_info = catalog->GetTableIndexes(table_info->name_);
  // 获取整张表的模式
  const auto& schema = table_info->schema_;
  
  TupleMeta tmeta{0, false};

  int insert_sum = 0;
  while(true) {
    // 对于insert算子，从next那里获取的元组只是一个临时元组
    // 该临时元组尚未被物化到数据库上，所以没有有效的rid
    const auto status = child_executor_->Next(&child_tuple,rid);
    if(!status) {
        break;
    }

    // 当元组被真正插入后，才真正物化到数据库中，此时返回其rid
    auto option_rid = table_heap->InsertTuple(tmeta, child_tuple);
    if(option_rid == std::nullopt) {
      continue;
    }

    // 插入成功
    child_tuple.SetRid(*option_rid);
    insert_sum++;
    
    // 每插入一个元组，就需要更新表中所有索引
    for(auto *info:index_info) {
      // 获取index对应的模式
      auto* index = info->index_.get();
      auto key_schema = index->GetKeySchema();
      const auto& key_attrs = index->GetKeyAttrs();

      auto key = child_tuple.KeyFromTuple(schema, *key_schema, key_attrs);
      info->index_->InsertEntry(key, *option_rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, insert_sum);
  *tuple = Tuple(values, &GetOutputSchema());
  rid->Set(INVALID_PAGE_ID, 0);

  is_finish_ = true;
  return true;
}

}  // namespace bustub
