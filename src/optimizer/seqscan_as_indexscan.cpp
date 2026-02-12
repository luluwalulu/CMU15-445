#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeProjection(child));
  }

  // CloneWithChildren的作用是保留当前节点的所有配置逻辑参数，但是将孩子替换为递归深入后，经优化返回的孩子
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    // const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto *seq_scan_plan = dynamic_cast<SeqScanPlanNode *>(optimized_plan.get());
    if (!seq_scan_plan) {
      return optimized_plan;
    }

    // 需要检查顺序扫描节点是否有谓词
    // 该谓词必须是ComparisonExpression
    // 该谓词必须有对应的索引，对于索引，容易获知其列号。但对于谓词，则需要获取其child的列号
    if (seq_scan_plan->filter_predicate_) {
      const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan_plan->filter_predicate_.get());
      if (cmp_expr == nullptr || cmp_expr->comp_type_ != ComparisonType::Equal) {
        return optimized_plan;
      }

      // 左孩子Value为ColumnValue，其中包含列号的信息
      const ColumnValueExpression *col_expr =
          dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
      BUSTUB_ASSERT(col_expr, "比较表达式中左孩子不为ColumnValue");
      auto colidx = col_expr->GetColIdx();

      auto val_expr = cmp_expr->GetChildAt(1);

      table_oid_t oid = seq_scan_plan->GetTableOid();
      auto table_info = catalog_.GetTable(oid);
      auto index_info = catalog_.GetTableIndexes(table_info->name_);

      // 模式本质上由column（列）组成，如果index_schema和索引的模式相同，即在该列上同时存在谓词和索引
      for (auto *info : index_info) {
        const auto &attrs = info->index_->GetKeyAttrs();
        if (attrs.size() == 1 && attrs[0] == colidx) {
          return std::make_shared<IndexScanPlanNode>(seq_scan_plan->output_schema_, seq_scan_plan->GetTableOid(),
                                                     info->index_oid_, nullptr, val_expr);
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
