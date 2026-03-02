//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();

  Tuple left_tuple{}, right_tuple{};
  RID left_rid{}, right_rid{};

  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();
  const auto &final_schema = GetOutputSchema();

  std::vector<Tuple> right_tuples{};

  while (true) {
    const auto status = left_executor_->Next(&left_tuple, &left_rid);
    if (!status) {
      break;
    }

    // std::cout<<"获取一次left_tuple"<<std::endl;
    std::vector<Value> values;

    for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
      values.push_back(left_tuple.GetValue(&left_schema, i));
    }

    right_executor_->Init();

    bool match{false};

    while (true) {
      const auto status = right_executor_->Next(&right_tuple, &right_rid);
      if (!status) {
        break;
      }

      // 该表达式既处理了ON，也处理了WHERE
      bool b = plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>();

      if (b) {
        auto final_values = values;

        for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
          final_values.push_back(right_tuple.GetValue(&right_schema, i));
        }

        Tuple final_tuple(final_values, &final_schema);
        results_.push_back(final_tuple);
        match = true;
      }
    }

    if (plan_->GetJoinType() == JoinType::LEFT && !match) {
      auto final_values = values;

      for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
        final_values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }

      Tuple final_tuple(final_values, &final_schema);
      results_.push_back(final_tuple);
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ < results_.size()) {
    *tuple = results_[cursor_++];
    rid->Set(INVALID_PAGE_ID, 0);
    return true;
  }

  return false;
}

}  // namespace bustub
