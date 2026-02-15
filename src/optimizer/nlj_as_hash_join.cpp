#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void GetKeyExpr(std::vector<AbstractExpressionRef> &left_key_expressions_,
                std::vector<AbstractExpressionRef> &right_key_expressions_, AbstractExpressionRef filter_expr) {
  std::vector<AbstractExpressionRef> children;
  // 最终递归到ColumnValueExpression，该表达式没有孩子，递归结束，进入下面的处理阶段
  for (const auto &child : filter_expr->GetChildren()) {
    GetKeyExpr(left_key_expressions_, right_key_expressions_, child);
  }
  // 该处理阶段实则只针对递归树的叶子节点，即ColumnValueExpression
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(filter_expr.get());
      column_value_expr != nullptr) {
    // 归于left_key_expression
    if (column_value_expr->GetTupleIdx() == 0) {
      left_key_expressions_.push_back(filter_expr);
      return;
    } else if (column_value_expr->GetTupleIdx() == 1) {
      right_key_expressions_.push_back(filter_expr);
      return;
    }

    throw bustub::Exception("tuple_idx not in range");
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(optimized_plan->children_.size() == 2, "NLJ should have 2 children");

    std::vector<AbstractExpressionRef> left_key_expressions_;
    std::vector<AbstractExpressionRef> right_key_expressions_;

    GetKeyExpr(left_key_expressions_, right_key_expressions_, nlj_plan.Predicate());

    return std::make_shared<HashJoinPlanNode>(optimized_plan->output_schema_, optimized_plan->GetChildAt(0),
                                              optimized_plan->GetChildAt(1), left_key_expressions_,
                                              right_key_expressions_, nlj_plan.GetJoinType());
  }

  return optimized_plan;
}

}  // namespace bustub
