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
auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finish_) {
    return false;
  }

  std::vector<RID> rids;
  Tuple child_tuple{};

  auto catalog = exec_ctx_->GetCatalog();
  auto table_heap = table_info_->table_.get();
  auto index_infos = catalog->GetTableIndexes(table_info_->name_);
  const auto &schema = table_info_->schema_;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  TupleMeta new_meta{txn->GetTransactionId(), false};
  Tuple new_tuple{};
  int update_sum = 0;

  while (true) {
    const auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      break;
    }

    rids.push_back(*rid);
  }

  for (auto r : rids) {
    auto pii = table_heap->GetTuple(r);
    auto base_meta = pii.first;
    auto base_tuple = pii.second;
    auto base_ts = base_meta.ts_;

    std::vector<Value> values;
    std::vector<Value> partial_values;
    std::vector<Column> partial_columns;
    std::vector<bool> modified_fields;

    if (base_ts >= TXN_START_ID && base_ts == txn->GetTransactionId()) {
      // 自我更新，更新表堆元组并更新当前事务的撤销日志
      auto old_undo_link = txn_mgr->GetUndoLink(r);
      // 如果old_undo_link为空，说明该元组刚刚被当前事务插入，当前版本我们不需要生成undo_log
      // 直接修改完值就OK
      if (!old_undo_link || !old_undo_link->IsValid()) {
        for (size_t i = 0; i < plan_->target_expressions_.size(); i++) {
          const auto expr = plan_->target_expressions_[i];
          auto v = expr->Evaluate(&base_tuple, schema);
          values.push_back(v);
        }
        new_tuple = {values, &schema};
        new_tuple.SetRid(base_tuple.GetRid());
        table_heap->UpdateTupleInPlace(new_meta, new_tuple, r, nullptr);
        update_sum++;
        continue;
      }
      auto old_log = txn->GetUndoLog(old_undo_link->prev_log_idx_);
      std::vector<Column> old_partial_columns;
      for (size_t i = 0; i < schema.GetColumnCount(); i++) {
        old_partial_columns.push_back(schema.GetColumn(i));
      }
      Schema temp_schema(old_partial_columns);

      for (size_t i = 0, j = 0; i < plan_->target_expressions_.size(); i++) {
        const auto expr = plan_->target_expressions_[i];
        auto v = expr->Evaluate(&base_tuple, schema);
        Value old_value{};
        // 如果上一次修改对元组的第i列进行了修改，那么必须在old_log中去取对应的Value，然后比较它和新的value是否相等
        // 反之，我们只需要获取base_tuple的第i列即可，并与之比较即可
        if (old_log.modified_fields_[i]) {
          old_value = old_log.tuple_.GetValue(&temp_schema, j++);
        } else {
          old_value = base_tuple.GetValue(&schema, i);
        }

        if (v.CompareExactlyEquals(old_value)) {
          modified_fields.push_back(false);
        } else {
          modified_fields.push_back(true);
          partial_values.push_back(v);
          partial_columns.push_back(schema.GetColumn(i));
        }
        values.push_back(v);
      }
      new_tuple = {values, &schema};
      new_tuple.SetRid(base_tuple.GetRid());
      if (IsTupleContentEqual(base_tuple, new_tuple)) {
        continue;
      }

      Schema partial_schema(partial_columns);
      Tuple partial_tuple(partial_values, &partial_schema);
      UndoLog new_log{old_log.is_deleted_, modified_fields, partial_tuple, old_log.ts_, old_log.prev_version_};

      table_heap->UpdateTupleInPlace(new_meta, new_tuple, r, nullptr);
      txn->ModifyUndoLog(old_undo_link->prev_log_idx_, new_log);
      update_sum++;
    } else if(base_ts < TXN_START_ID && base_ts<= txn->GetReadTs()) {
      // 正常修改，需要删除原来的元组，插入新的元组。同时生成撤销日志并插入
      for (size_t i = 0; i < plan_->target_expressions_.size(); i++) {
        const auto expr = plan_->target_expressions_[i];
        auto v = expr->Evaluate(&base_tuple, schema);
        if (v.CompareExactlyEquals(base_tuple.GetValue(&schema, i))) {
          modified_fields.push_back(false);
        } else {
          modified_fields.push_back(true);
          partial_values.push_back(v);
          partial_columns.push_back(schema.GetColumn(i));
        }
        values.push_back(v);
      }
      new_tuple = {values, &schema};
      new_tuple.SetRid(base_tuple.GetRid());
      if (IsTupleContentEqual(base_tuple, new_tuple)) {
        continue;
      }

      // UndoLog中存储的是base_tuple相关的信息
      Schema partial_schema(partial_columns);
      Tuple partial_tuple(partial_values, &partial_schema);
      // 如果堆元组之前的头UndoLink可能有效可能无效
      auto opt_undo_link = txn_mgr->GetUndoLink(r);
      UndoLink undo_link{};
      if (opt_undo_link && opt_undo_link->IsValid()) {
        undo_link = *opt_undo_link;
      } 
      UndoLog undo_log{false, modified_fields, partial_tuple, base_meta.ts_, undo_link};
      auto new_undo_link = txn->AppendUndoLog(std::move(undo_log));
      txn_mgr->UpdateUndoLink(r, std::make_optional<UndoLink>(new_undo_link), nullptr);

      // base_meta.is_deleted_ = true;
      // table_heap->UpdateTupleMeta(base_meta, r);
      // auto opt_rid = table_heap->InsertTuple(new_meta, new_tuple);
      // if (opt_rid == std::nullopt) {
      //   throw "update执行器插入失败";
      // }
      // r = *opt_rid;
      new_tuple.SetRid(r);
      table_heap->UpdateTupleInPlace(new_meta, new_tuple, r, nullptr);
      update_sum++;
      txn->AppendWriteSet(table_info_->oid_, r);
    } else {
      // 写写冲突
      txn->SetTainted();
      throw ExecutionException("update执行器发生了写写冲突");
    }

    // 统一处理索引
    for (auto *info : index_infos) {
      auto *index = info->index_.get();
      auto key_schema = index->GetKeySchema();
      const auto &key_attrs = index->GetKeyAttrs();
      auto old_key = base_tuple.KeyFromTuple(schema, *key_schema, key_attrs);
      auto new_key = new_tuple.KeyFromTuple(schema, *key_schema, key_attrs);

      info->index_->DeleteEntry(old_key, r, exec_ctx_->GetTransaction());
      info->index_->InsertEntry(new_key, r, exec_ctx_->GetTransaction());
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
