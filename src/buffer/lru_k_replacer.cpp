//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {
// LRUKNode中添加的功能
auto LRUKNode::GetEvictable() const -> bool { return is_evictable_; }

void LRUKNode::SetEvictable(bool evictable) { is_evictable_ = evictable; }

auto LRUKNode::Size() const -> size_t { return history_.size(); }

auto LRUKNode::EarliestStamp() const -> size_t { return history_.front(); }

void LRUKNode::Insert(size_t curr_timestamp, size_t k) {
  if (history_.size() == k) {
    history_.erase(history_.begin());
  }
  history_.emplace_back(curr_timestamp);
}

// LRUKReplacer中应该实现的功能

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();

  if (node_store_.empty()) {
    latch_.unlock();
    return false;
  }

  frame_id_t inf_fid = -1;
  size_t inf_earliest_stamp;

  frame_id_t max_fid;
  size_t max_kdist = 0;
  for (const auto &p : node_store_) {
    auto &knode = p.second;
    auto &fid = p.first;

    // 不能驱逐
    if (!knode.GetEvictable()) {
      continue;
    }

    // 对应inf
    if (knode.Size() != k_) {
      if (inf_fid == -1 || knode.EarliestStamp() < inf_earliest_stamp) {
        inf_fid = fid;
        inf_earliest_stamp = knode.EarliestStamp();
      }
      continue;
    }

    if (current_timestamp_ - knode.EarliestStamp() > max_kdist) {
      max_kdist = current_timestamp_ - knode.EarliestStamp();
      max_fid = fid;
    }
  }

  frame_id_t erase_fid;
  if (inf_fid == -1) {
    erase_fid = max_fid;
  } else {
    erase_fid = inf_fid;
  }

  curr_size_--;
  node_store_.erase(erase_fid);
  *frame_id = erase_fid;

  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();

  if (static_cast<uint32_t>(frame_id) > replacer_size_) {
    latch_.unlock();
    throw Exception("LRUKReplacer::RecordAccess");
  }

  node_store_[frame_id].Insert(current_timestamp_, k_);
  current_timestamp_++;

  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    latch_.unlock();
    throw Exception("LRUKReplacer::SetEvictable");
  }
  bool old = it->second.GetEvictable();
  if (old != set_evictable) {
    it->second.SetEvictable(!old);
    if (old) {
      curr_size_--;
    } else {
      curr_size_++;
    }
  }

  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 在进行状态检查之前需要加锁
  latch_.lock();

  if (node_store_.find(frame_id) == node_store_.end()) {
    latch_.unlock();
    return;
  }

  auto knode = node_store_[frame_id];
  if (!node_store_[frame_id].GetEvictable()) {
    latch_.unlock();
    throw Exception("LRUKReplacer::Remove");
  }

  node_store_.erase(frame_id);
  curr_size_--;

  // 状态彻底修改完成之后才能释放锁
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  auto i = curr_size_;
  latch_.unlock();
  return i;
}

}  // namespace bustub
