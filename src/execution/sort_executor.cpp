#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple t;
  RID r;

  while (true) {
    const auto status = child_executor_->Next(&t, &r);
    if (!status) {
      break;
    }

    results_.push_back(t);
  }

  sort(results_.begin(), results_.end(), [this](const Tuple &t1, const Tuple &t2) -> bool {
    auto order_bys = plan_->GetOrderBy();

    for (auto p : order_bys) {
      auto t1_value = p.second->Evaluate(&t1, GetOutputSchema());
      auto t2_value = p.second->Evaluate(&t2, GetOutputSchema());

      if (p.first == OrderByType::DEFAULT || p.first == OrderByType::ASC) {
        // 升序，升就返回true，降就返回false，等就继续遍历
        if (t1_value.CompareLessThan(t2_value) == CmpBool::CmpTrue) {
          return true;
        } else if (t1_value.CompareGreaterThan(t2_value) == CmpBool::CmpTrue) {
          return false;
        }
      } else if (p.first == OrderByType::DESC) {
        // 降序，降就返回true，升就返回false，等就继续遍历
        if (t1_value.CompareGreaterThan(t2_value) == CmpBool::CmpTrue) {
          return true;
        } else if (t1_value.CompareLessThan(t2_value) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }

    // 如果依照排序两者完全相等的话，则不应该交换顺序，返回true表明本来的顺序符合要求无需交换即可。
    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == results_.size()) {
    return false;
  }

  *tuple = results_[cursor_++];
  *rid = tuple->GetRid();

  return true;
}

}  // namespace bustub
