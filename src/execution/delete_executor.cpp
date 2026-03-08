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

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  } 

  Tuple base_tuple{};
  Tuple log_tuple{};
  TupleMeta base_meta{};
  TupleMeta new_meta{};
  int delete_sum = 0;

  auto catalog = exec_ctx_->GetCatalog();
  table_oid_t oid = plan_->GetTableOid();
  auto table_info = catalog->GetTable(oid);
  auto table_heap = table_info->table_.get();
  auto index_info = catalog->GetTableIndexes(table_info->name_);
  const auto &schema = table_info->schema_;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  new_meta = {txn->GetTransactionId(), true};

  while (true) {
    const auto status = child_executor_->Next(&base_tuple, rid);
    if (!status) {
      break;
    }
    // std::cout<<r.ToString()<<std::endl;
    // std::cout<<rid->ToString()<<std::endl;
    auto r = *rid;
    auto pii = table_heap->GetTuple(r);
    base_tuple = pii.second;
    base_meta = pii.first;
    auto base_ts = base_meta.ts_;

    std::vector<Value> values;
    std::vector<bool> modified_fields;

    if (base_ts >= TXN_START_ID && base_ts == txn->GetTransactionId()) {
      // 自我更新，此时表堆元组直接更新为删除状态即可,撤销日志需要保存所有的Value
      // std::cout<<"情况一"<<std::endl;
      auto opt_undo_link = txn_mgr->GetUndoLink(r);
      if (!opt_undo_link || !opt_undo_link->IsValid()) {
        if (!opt_undo_link->IsValid()) {
          std::cout<<"能够获取UndoLink但是是无效Link"<<std::endl;
        }
        table_heap->UpdateTupleMeta({0, true}, r);
        continue;
      }
      auto old_log = txn->GetUndoLog(opt_undo_link->prev_log_idx_);
      std::vector<Column> old_partial_columns;
      for (size_t i = 0; i < schema.GetColumnCount(); i++) {
        old_partial_columns.push_back(schema.GetColumn(i));
      }
      Schema temp_schema(old_partial_columns);

      for (size_t i = 0, j = 0; i < schema.GetColumnCount(); i++) {
        Value old_value{};
        // 如果上一次修改对元组的第i列进行了修改，那么必须在old_log中去取对应的Value，然后比较它和新的value是否相等
        // 反之，我们只需要获取base_tuple的第i列即可，并与之比较即可
        if (old_log.modified_fields_[i]) {
          old_value = old_log.tuple_.GetValue(&temp_schema, j++);
        } else {
          old_value = base_tuple.GetValue(&schema, i);
        }
        modified_fields.push_back(true);
        values.push_back(old_value);
      }
      log_tuple = {values, &schema};
      log_tuple.SetRid(base_tuple.GetRid());
      UndoLog new_log{old_log.is_deleted_, modified_fields, log_tuple, old_log.ts_, old_log.prev_version_};

      table_heap->UpdateTupleMeta(new_meta, r);
      txn->ModifyUndoLog(opt_undo_link->prev_log_idx_, new_log);
    } else if(base_ts < TXN_START_ID && base_ts <= txn->GetReadTs()) {
      // 同样将堆上元组直接删除，但是需要插入新的撤销日志
      // std::cout<<"情况二"<<std::endl;
      modified_fields.assign(schema.GetColumnCount(), true);
      auto opt_undo_link = txn_mgr->GetUndoLink(r);
      UndoLink undo_link{};
      if (opt_undo_link && opt_undo_link->IsValid()) {
        undo_link = *opt_undo_link;
      } 
      // std::cout<<"正常修改，堆元组的提交时间戳为"<<base_meta.ts_<<std::endl;
      UndoLog undo_log{false, modified_fields, base_tuple, base_meta.ts_, undo_link};
      auto new_undo_link = txn->AppendUndoLog(std::move(undo_log));
      txn_mgr->UpdateUndoLink(r, std::make_optional<UndoLink>(new_undo_link), nullptr);

      table_heap->UpdateTupleMeta(new_meta, r);
      txn->AppendWriteSet(table_info->oid_, r);
    } else {
      // 写写冲突
      // std::cout<<"情况三"<<std::endl;
      txn->SetTainted();
      throw ExecutionException("update执行器发生了写写冲突");
    }

    delete_sum++;

    for (auto *info : index_info) {
      auto *index = info->index_.get();
      auto key_schema = index->GetKeySchema();
      const auto &key_attrs = index->GetKeyAttrs();
      auto key = base_tuple.KeyFromTuple(schema, *key_schema, key_attrs);

      info->index_->DeleteEntry(key, r, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> return_values;
  return_values.emplace_back(TypeId::INTEGER, delete_sum);
  *tuple = Tuple(return_values, &GetOutputSchema());
  rid->Set(INVALID_PAGE_ID, 0);

  is_finished_ = true;
  return true;
}

}  // namespace bustub
