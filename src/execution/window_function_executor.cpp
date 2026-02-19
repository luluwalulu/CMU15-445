#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  // 一.先进行排序
  child_executor_->Init();
  std::vector<Tuple> results;

  Tuple t;
  RID r;

  while (true) {
    const auto status = child_executor_->Next(&t, &r);
    if (!status) {
      break;
    }

    results.push_back(t);
  }

  if (!plan_->window_functions_.empty()) {
    const auto &window_func = plan_->window_functions_.begin()->second;
    const auto &order_bys = window_func.order_by_;

    std::sort(results.begin(), results.end(), [&order_bys, this](const Tuple &t1, const Tuple &t2) -> bool {
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

  // 二.为每个分区生成初始值
  // aggregate_values数组内存储着聚合值，results[i]中元组对应的聚合值存储在aggregate_values[i]中
  std::vector<std::vector<Value>> aggregate_values;
  std::unordered_map<GroupByKey, AggregateValues> htable{};

  for (const auto &p : plan_->window_functions_) {
    const auto &window_func = p.second;
    const auto &partition_bys = window_func.partition_by_;
    const auto &function = window_func.function_;
    const auto &type = window_func.type_;
    const auto &order_by = window_func.order_by_;

    if (type != WindowFunctionType::Rank) {
      // 将所有元组插入到htable中
      for (const auto &tuple : results) {
        std::vector<Value> keys;
        for (const auto &expr : partition_bys) {
          keys.emplace_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
        }
        // 得到键
        GroupByKey key{keys};

        std::vector<Value> values;
        values.emplace_back(function->Evaluate(&tuple, child_executor_->GetOutputSchema()));
        // 得到插入值时的参照物
        AggregateValues input{values};

        // 插入值
        if (htable.count(key) == 0) {
          htable.insert({key, GenerateInitialAggregateValue(htable, type)});
        }

        CombineAggregateValues(&htable[key], input, htable, type);
      }

      // 插入完成后，提取聚合函数所得到aggregate_values中
      for (size_t i = 0; i < results.size(); i++) {
        std::vector<Value> keys;
        for (const auto &expr : partition_bys) {
          keys.emplace_back(expr->Evaluate(&results[i], child_executor_->GetOutputSchema()));
        }
        GroupByKey group_by_key{keys};
        aggregate_values[i].emplace_back(htable[group_by_key].aggregates_[0]);
      }
    } else {
      std::unordered_map<GroupByKey, std::tuple<int, uint32_t, Tuple>> rank_table;

      for (size_t i = 0; i < results.size(); i++) {
        std::vector<Value> keys;
        for (const auto &expr : partition_bys) {
          keys.emplace_back(expr->Evaluate(&results[i], child_executor_->GetOutputSchema()));
        }
        // 得到键
        GroupByKey key{keys};

        if (rank_table.count(key) == 0) {
          rank_table.insert({key, {-1, 1, {}}});
        }

        auto &last_rank = std::get<0>(rank_table[key]);
        auto &actual_rank = std::get<1>(rank_table[key]);
        auto &last_tuple = std::get<2>(rank_table[key]);

        // 如果和前一个元组按照排序的优先级来讲相同
        if (last_rank != -1 && IsTupleEqual(last_tuple, results[i], order_by)) {
          // 得到last_rank
          aggregate_values[i].emplace_back(ValueFactory::GetIntegerValue(last_rank));
          actual_rank++;
        } else {
          // 得到actual_rank
          aggregate_values[i].emplace_back(ValueFactory::GetIntegerValue(actual_rank));
          last_rank = actual_rank;
          actual_rank++;
          last_tuple = results[i];
        }
      }  // for循环
    }    // else分支
  }      // 遍历窗口函数的for循环

  // 三.遍历完所有窗口函数中，所有results[i]对应元组所需要的聚合信息，都在aggregate_values[i]中
  for (size_t i = 0; i < results.size(); i++) {
    // 将results[i]中信息和aggregate_values[i]中信息整合
    size_t ori_row = 0;
    size_t aggregate_row = 0;
    size_t row;
    const auto &tuple = results[i];

    std::vector<Value> values;

    for (size_t j = 0; j < GetOutputSchema().GetColumnCount(); j++) {
      // 说明第j列应该是原来的元组对应的信息
      if (plan_->window_functions_.count(j) == 0) {
        row = ori_row++;
        auto value = tuple.GetValue(&child_executor_->GetOutputSchema(), row);
        values.push_back(value);
      } else {
        row = aggregate_row++;
        values.push_back(aggregate_values[i][aggregate_row]);
      }
    }

    final_.push_back({values, &GetOutputSchema()});
  }
}  // Init函数结尾

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == final_.size()) {
    return false;
  }

  *tuple = final_[cursor_++];
  *rid = tuple->GetRid();

  return true;
}
}  // namespace bustub
