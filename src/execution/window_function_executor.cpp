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
  std::vector<std::unordered_map<uint32_t, Value>> aggregate_values(results.size());
  std::unordered_map<GroupByKey, AggregateValues> htable{};
  bool orderby_isempty{true};

  for (const auto &p : plan_->window_functions_) {
    const auto &col_idx = p.first;
    const auto &window_func = p.second;
    const auto &partition_bys = window_func.partition_by_;
    const auto &function = window_func.function_;
    const auto &type = window_func.type_;
    const auto &order_by = window_func.order_by_;
    if (!order_by.empty()) {
      orderby_isempty = false;
    }

    if (type != WindowFunctionType::Rank) {
      // 将所有元组插入到htable中
      for (size_t i = 0; i < results.size(); i++) {
        const auto &tuple = results[i];
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

        if (!orderby_isempty) {
          aggregate_values[i][col_idx] = htable[key].aggregates_[0];
        }
      } // 遍历元组

      if (orderby_isempty) {
        for (size_t i = 0; i < results.size(); i++) {
          std::vector<Value> keys;
          for (const auto &expr : partition_bys) {
            keys.emplace_back(expr->Evaluate(&results[i], child_executor_->GetOutputSchema()));
          }

          GroupByKey group_by_key{keys};

          if (htable.find(group_by_key) == htable.end()) {
            throw Exception("group_by_key本应已经插入");
          }

          aggregate_values[i][col_idx] = (htable[group_by_key].aggregates_[0]);
        }
      }

      htable.clear();
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
          auto v = ValueFactory::GetIntegerValue(last_rank);
          aggregate_values[i][col_idx] = v;
          actual_rank++;
        } else {
          // 得到actual_rank
          auto v = ValueFactory::GetIntegerValue(actual_rank);
          aggregate_values[i][col_idx] = v;
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
    const auto &tuple = results[i];

    std::vector<Value> values;

    for (size_t j = 0; j < GetOutputSchema().GetColumnCount(); j++) {
      // 说明第j列应该是原来的元组对应的信息
      if (plan_->window_functions_.count(j) == 0) {
        auto expr = plan_->columns_[j].get();
        values.push_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
      } else {
        values.push_back(aggregate_values[i][j]);
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
