#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  current_reads_[read_ts]++;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  current_reads_[read_ts]--;
  if (current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
  }

  while (current_reads_.find(watermark_) == current_reads_.end()) {
    // 需要更新watermark_，在current_reads当中寻找
    watermark_++;
  }
}

}  // namespace bustub
