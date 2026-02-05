//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager


  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // 先获取缓冲区的槽位，即frame_id_t
  // 先尝试从freelist获取，不然尝试驱逐
  frame_id_t frame_id = -1;
  latch_.lock();

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.erase(free_list_.begin());
  } else {
    // 驱逐失败
    if (!replacer_->Evict(&frame_id)) {
      frame_id = -1;
    }
  }

  // 从freelist获取 和 驱逐都尝试失败
  if (frame_id == -1) {
    latch_.unlock();
    return nullptr;
  }

  // 腾出槽位后,先将旧的信息刷回磁盘，然后再将新的page放入对应位置
  auto next_id = AllocatePage();
  auto &page = pages_[frame_id];

  if (page.IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto futrue = promise.get_future();
    disk_scheduler_->Schedule({true, page.data_, page.page_id_, std::move(promise)});
    futrue.get();
  }

  // 还应该在page_table_中删除旧的page_id
  page_table_.erase(page.page_id_);
  page_table_.emplace(next_id, frame_id);

  // 初始化data和metadata
  page.ResetMemory();
  page.page_id_ = next_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  latch_.unlock();

  *page_id = next_id;
  return &page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // 从磁盘中读取页号为page_id的页，该页可能仍然存在，也有可能之前已经被替换了
  latch_.lock();

  frame_id_t frame_id;

  // 如果该page仍然存在，直接返回就行
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    auto &page = pages_[frame_id];
    page.pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    latch_.unlock();
    return &page;
  }

  // 如果该page已经不存在
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.erase(free_list_.begin());
  } else {
    // 驱逐失败
    if (!replacer_->Evict(&frame_id)) {
      latch_.unlock();
      return nullptr;
    }
  }

  // 腾出槽位后,先将旧的信息刷回磁盘，然后再将对应page放入对应位置中
  auto &page = pages_[frame_id];

  if (page.IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto futrue = promise.get_future();
    disk_scheduler_->Schedule({true, page.data_, page.page_id_, std::move(promise)});
    futrue.get();
  }

  page_table_.erase(page.page_id_);
  page_table_.emplace(page_id, frame_id);

  auto promise = disk_scheduler_->CreatePromise();
  auto futrue = promise.get_future();
  disk_scheduler_->Schedule({false, page.data_, page_id, std::move(promise)});

  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  
  futrue.get();

  latch_.unlock();

  return &page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();

  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  auto frame_id = page_table_[page_id];
  auto &page = pages_[frame_id];
  if (page.pin_count_ == 0) {
    latch_.unlock();
    return false;
  }

  if (--page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  page.is_dirty_ = is_dirty;

  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // DiskRequest r{true,(char*)pages_[]}
  // disk_scheduler_->Schedule();
  latch_.lock();

  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  auto promise = disk_scheduler_->CreatePromise();
  auto futrue = promise.get_future();
  auto frame_id = page_table_[page_id];
  auto &page = pages_[frame_id];

  disk_scheduler_->Schedule({true, page.data_, page_id, std::move(promise)});

  futrue.get();

  page.is_dirty_ = false;

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();

  for (auto p : page_table_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto futrue = promise.get_future();
    auto page_id = p.first;
    auto frame_id = p.second;
    auto &page = pages_[frame_id];

    disk_scheduler_->Schedule({true, page.data_, page_id, std::move(promise)});

    // 每一次都强制等到接收值后才到下一次遍历
    futrue.get();

    page.is_dirty_ = false;
  }

  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();

  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  auto frame_id = page_table_[page_id];
  auto &page = pages_[frame_id];

  if (page.pin_count_) {
    latch_.unlock();
    return false;
  }

  page_table_.erase(page_id);

  replacer_->Remove(frame_id);

  free_list_.emplace_back(frame_id);

  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;

  DeallocatePage(page_id);

  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
