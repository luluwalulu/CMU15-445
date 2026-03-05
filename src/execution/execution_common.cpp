#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // undo_logs中已经包含我们所需要的所有UndoLog
  // 举个例子，读时间戳为10的事务想要读取元组t，元组t的版本链为1，5，8，9，11，13，14
  // 则我们拥有的信息如下
  // 最新版本的元组base_tuple
  // undo_logs：包含提交号为9，11，13，14的事务中对应元组的UndoLog，最终回退到修改9对应的版本

  // 我们返回的是一个std::optional<Tuple>，这意味着，某些情况下，我们可能需要返回std::nullopt
  // 即对应这种情况，元组t在当前事务的快照中恰好处于被删除状态
  Tuple t(base_tuple);
  bool is_deleted{base_meta.is_deleted_};

  for (const auto &undo_log : undo_logs) {
    // 如果进行增量操作前后，元组都处于被删除状态，那么毫无意义
    if (is_deleted && undo_log.is_deleted_) {
      continue;
    }

    // 更新deleted状态
    is_deleted = undo_log.is_deleted_;

    // vector<bool>，用于指示哪些列被修改
    const auto &modified_fields = undo_log.modified_fields_;
    // 其中包含元组的被修改字段
    const auto &modified_tuple = undo_log.tuple_;
    // 用于构建进行撤销操作后的元组
    std::vector<Value> values;

    // 需要构建modified_tuple的模式才能获取其Value
    std::vector<Column> columns;
    for (size_t i = 0; i < modified_fields.size(); i++) {
      if (modified_fields[i]) {
        columns.push_back(schema->GetColumn(i));
      }
    }
    Schema partial_schema(columns);

    // 获取Value，并构建最终的Tuple
    int col_idx = 0;
    for (size_t i = 0; i < modified_fields.size(); i++) {
      if (modified_fields[i]) {
        // std::cout << "第" << i << "列字段被修改为" << modified_tuple.GetValue(&partial_schema, col_idx).ToString()
                  // << std::endl;
        values.push_back(modified_tuple.GetValue(&partial_schema, col_idx++));
      } else {
        // std::cout << "第" << i << "列字段保持原来的值" << t.GetValue(schema, i).ToString() << std::endl;
        values.push_back(t.GetValue(schema, i));
      }
    }
    t = {values, schema};
  }

  if (is_deleted) {
    return std::nullopt;
  }
  return std::make_optional<Tuple>(t);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
  TableIterator itr(table_info->table_->MakeIterator());

  while (!itr.IsEnd()) {
    auto pii = itr.GetTuple();

    auto meta = pii.first;
    auto t = pii.second;
    auto rid = t.GetRid();
    // 表示堆中元组的提交时间戳
    auto heap_ts = meta.ts_;
    // 表示版本链中的第一个UndoLink连接
    auto undo_link = txn_mgr->GetUndoLink(t.GetRid());
    // 表示当前版本的元组是否被删除
    bool is_deleted{meta.is_deleted_};
    // 存储回退版本
    std::vector<UndoLog> undo_logs;

    std::cout<<"RID"<<rid.GetPageId()<<'/'<<rid.GetSlotNum()<<' ';
    if (heap_ts > TXN_START_ID) {
      std::cout<<"ts=txn"<<heap_ts-TXN_START_ID<<' ';
    } else {
      std::cout<<"ts="<<heap_ts<<' ';
    }
    if (is_deleted) {
      std::cout<<"<del>"<<' ';
    }
    std::cout<<"tuple="<<t.ToString(&table_info->schema_)<<' ';
    // if (!undo_link) {
    //   std::cout<<"第一个节点没有UndoLink";
    // }
    std::cout<<std::endl;

    // 打印版本链
    while (undo_link.has_value() && undo_link->IsValid()) {
      std::cout<<"  ";
      auto undo_log = txn_mgr->GetUndoLog(*undo_link);
    
      undo_logs.push_back(undo_log);
      heap_ts = undo_log.ts_;
      
      is_deleted = undo_log.is_deleted_;
      if (!is_deleted) {
        t = *ReconstructTuple(&table_info->schema_, pii.second, meta, undo_logs);
      }

      std::cout<<"txn"<<undo_link->prev_txn_<<'@'<<undo_link->prev_log_idx_<<' ';
      if (is_deleted) {
        std::cout<<"<del>"<<' ';
      } else {
        std::cout<<t.ToString(&table_info->schema_)<<' ';
      }
      std::cout<<"ts="<<heap_ts<<std::endl;

      undo_link = undo_log.prev_version_;
    }

    ++itr;
  }
}

}  // namespace bustub
