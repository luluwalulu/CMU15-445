//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

// Init函数需要生成哈希表，然后将数据插入到哈希表中
void AggregationExecutor::Init() {
  Tuple t{};
  RID r{};

  child_executor_->Init();

  while (true) {
    const auto status = child_executor_->Next(&t, &r);
    if (!status) {
      break;
    }

    // std::cout<<"插入一个元组"<<std::endl;
    auto key_from_tuple = MakeAggregateKey(&t);
    auto value_from_tuple = MakeAggregateValue(&t);

    aht_.InsertCombine(key_from_tuple, value_from_tuple);
  }

  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    aht_.InsertInitialCombine();
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values;
  if (!plan_->GetGroupBys().empty()) {
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  }
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());

  *tuple = {values, &GetOutputSchema()};
  rid->Set(INVALID_PAGE_ID, 0);
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
