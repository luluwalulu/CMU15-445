#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (!page_) {
    return;
  }
  bpm_->UnpinPage(PageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();

  this->guard_ = std::move(that.guard_);

  return *this;
}

void ReadPageGuard::Drop() {
  if (!guard_.page_) {
    return;
  }
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();

  this->guard_ = std::move(that.guard_);

  return *this;
}

void WritePageGuard::Drop() {
  if (!guard_.page_) {
    return;
  }
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // 在升级期间页不能被驱逐，也就是pin_count始终不能为0
  // 之后原本的页应该无效，只能使用最新的页

  // 关键在于绝对不能调用unpin，因为资源自始至终只有一份，一旦中途调用了unpin，就意味着放弃了这份资源
  // 同时也不要给旧的BasicPageGuard调用Unpin的机会
  // 需要表现出当前BasicPageGuard资源已经被移空
  ReadPageGuard rg(bpm_, page_);
  page_->RLatch();
  bpm_ = nullptr;
  page_ = nullptr;
  return rg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard wg(bpm_, page_);
  page_->WLatch();
  bpm_ = nullptr;
  page_ = nullptr;
  return wg;
}

}  // namespace bustub
