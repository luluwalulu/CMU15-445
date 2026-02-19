#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      cmp_(plan_->GetOrderBy(), GetOutputSchema()) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  Tuple t;
  RID r;
  std::priority_queue<Tuple, std::vector<Tuple>, TopNCompare> queue(cmp_);

  while (true) {
    const auto status = child_executor_->Next(&t, &r);
    if (!status) {
      break;
    }

    if (queue.size() < plan_->GetN()) {
      queue.push(t);
    } else {
      const auto &top_tuple = queue.top();
      // 则说明新得到的t更靠近堆底
      if (cmp_(t, top_tuple)) {
        queue.pop();
        queue.push(t);
      }
    }
  }

  // 翻转queue_
  while (!queue.empty()) {
    results_.push_back(queue.top());
    queue.pop();
  }

  std::reverse(results_.begin(), results_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == results_.size()) {
    return false;
  }

  *tuple = results_[cursor_++];
  *rid = tuple->GetRid();

  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return results_.size(); };

}  // namespace bustub
