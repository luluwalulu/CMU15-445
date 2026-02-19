#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    auto child_plan = optimized_plan->GetChildAt(0);
    if (child_plan->GetType() != PlanType::Sort) {
      return optimized_plan;
    }

    // 新节点应该同时具有Limit和Sort的属性，其孩子节点为Sort的孩子节点
    auto *limit_plan = dynamic_cast<LimitPlanNode *>(optimized_plan.get());
    if (limit_plan == nullptr) {
      return optimized_plan;
    }

    auto *sort_plan = dynamic_cast<const SortPlanNode *>(child_plan.get());
    if (sort_plan == nullptr) {
      return optimized_plan;
    }

    return std::make_shared<TopNPlanNode>(limit_plan->output_schema_, sort_plan->GetChildAt(0), sort_plan->GetOrderBy(),
                                          limit_plan->GetLimit());
  }

  return optimized_plan;
}

}  // namespace bustub
