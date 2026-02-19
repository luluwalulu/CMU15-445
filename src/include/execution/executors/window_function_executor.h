//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "execution/expressions/column_value_expression.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialAggregateValue(const std::unordered_map<GroupByKey, AggregateValues> &htable,
                                     const WindowFunctionType &type) -> AggregateValues {
    std::vector<Value> values{};
    switch (type) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        values.emplace_back(ValueFactory::GetIntegerValue(0));
        break;
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        // Others starts at null.
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        break;
      default:
        break;
    }

    return {values};
  }

  void CombineAggregateValues(AggregateValues *result, const AggregateValues &input,
                              const std::unordered_map<GroupByKey, AggregateValues> &htable,
                              const WindowFunctionType &type) {
    // 一个分组中有i个聚合目标，我插入的一个元组需要依次修改这i个聚合目标。第i个聚合目标对应result->aggregates[i]
    // input是一个元组有关这i个聚合目标的i个有用信息，input.aggregates_[i]对于result->aggregates[i]的修改有着参考作用
    int old_val_int = result->aggregates_[0].GetAs<int>();
    auto new_Value = input.aggregates_[0];
    int new_val_int = input.aggregates_[0].GetAs<int>();

    switch (type) {
      case WindowFunctionType::CountStarAggregate: {
        result->aggregates_[0] = ValueFactory::GetIntegerValue(old_val_int + 1);
        break;
      }
      case WindowFunctionType::CountAggregate: {
        if (new_val_int == BUSTUB_INT32_NULL) {
          break;
        }

        if (old_val_int == BUSTUB_INT32_NULL) {
          old_val_int = 0;
        }
        if (!new_Value.IsNull()) {
          result->aggregates_[0] = ValueFactory::GetIntegerValue(old_val_int + 1);
        }

        break;
      }
      case WindowFunctionType::SumAggregate: {
        if (new_val_int == BUSTUB_INT32_NULL) {
          break;
        }

        if (old_val_int == BUSTUB_INT32_NULL) {
          old_val_int = 0;
        }
        result->aggregates_[0] = ValueFactory::GetIntegerValue(new_val_int + old_val_int);

        break;
      }
      case WindowFunctionType::MinAggregate: {
        if (new_val_int == BUSTUB_INT32_NULL) {
          break;
        }

        if (old_val_int == BUSTUB_INT32_NULL) {
          result->aggregates_[0] = ValueFactory::GetIntegerValue(new_val_int);
          break;
        }
        result->aggregates_[0] = ValueFactory::GetIntegerValue(std::min(new_val_int, old_val_int));

        break;
      }
      case WindowFunctionType::MaxAggregate: {
        if (new_val_int == BUSTUB_INT32_NULL) {
          break;
        }

        if (old_val_int == BUSTUB_INT32_NULL) {
          result->aggregates_[0] = ValueFactory::GetIntegerValue(new_val_int);
          break;
        }
        result->aggregates_[0] = ValueFactory::GetIntegerValue(std::max(new_val_int, old_val_int));

        break;
      }
      default:
        break;
    }
  }

  bool IsTupleEqual(const Tuple &t1, const Tuple &t2,
                    const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by) {
    for (auto p : order_by) {
      auto t1_value = p.second->Evaluate(&t1, GetOutputSchema());
      auto t2_value = p.second->Evaluate(&t2, GetOutputSchema());

      if (p.first == OrderByType::DEFAULT || p.first == OrderByType::ASC) {
        // 升序，升就返回true，降就返回false，等就继续遍历
        if (t1_value.CompareLessThan(t2_value) == CmpBool::CmpTrue) {
          return false;
        } else if (t1_value.CompareGreaterThan(t2_value) == CmpBool::CmpTrue) {
          return false;
        }
      } else if (p.first == OrderByType::DESC) {
        // 降序，降就返回true，升就返回false，等就继续遍历
        if (t1_value.CompareGreaterThan(t2_value) == CmpBool::CmpTrue) {
          return false;
        } else if (t1_value.CompareLessThan(t2_value) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }

    return true;
  }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> final_{};

  size_t cursor_{0};
};
}  // namespace bustub
