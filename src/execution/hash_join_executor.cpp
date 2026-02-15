//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple right_tuple{};
  RID right_rid{};

  // 完成哈希表的初始化
  while (true) {
    const auto status = right_executor_->Next(&right_tuple, &right_rid);
    if (!status) {
      break;
    }

    auto right_key = MakeRightJoinKey(&right_tuple);

    htable_[right_key].push_back(right_tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values;

  // 如果记录有tuple_vec_，且下标没超范围
  if (tuple_vec_ != nullptr && cursor_ < tuple_vec_->size()) {
    values = left_values_;
    auto right_tuple = tuple_vec_->operator[](cursor_++);

    for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
      values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
    }

    *tuple = {values, &GetOutputSchema()};
    *rid = tuple->GetRid();
    return true;
  }

  // 如果需要重新获取left_tuple_
  while (true) {
    const auto status = left_executor_->Next(tuple, rid);
    if (!status) {
      return false;
    }

    auto left_key = MakeLeftJoinKey(tuple);

    if (htable_.find(left_key) != htable_.end()) {
      cursor_ = 0;
      tuple_vec_ = &htable_[left_key];

      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(tuple->GetValue(&left_executor_->GetOutputSchema(), i));
      }

      left_values_ = values;

      auto right_tuple = tuple_vec_->operator[](cursor_++);

      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
      }

      *tuple = {values, &GetOutputSchema()};
      *rid = tuple->GetRid();
      return true;
    } else if (htable_.find(left_key) == htable_.end() && plan_->GetJoinType() == JoinType::LEFT) {
      cursor_ = 0;
      tuple_vec_ = nullptr;

      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(tuple->GetValue(&left_executor_->GetOutputSchema(), i));
      }

      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }

      *tuple = {values, &GetOutputSchema()};
      *rid = tuple->GetRid();
      return true;
    }
  }
}

}  // namespace bustub
