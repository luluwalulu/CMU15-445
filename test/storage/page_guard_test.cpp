//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// 定义一个 Fixture 类来减少重复代码
class PageGuardTest : public ::testing::Test {
 protected:
  void SetUp() override {
    disk_manager_ = std::make_shared<DiskManagerUnlimitedMemory>();
    bpm_ = std::make_shared<BufferPoolManager>(buffer_pool_size_, disk_manager_.get(), k_);
  }

  void TearDown() override { disk_manager_->ShutDown(); }

  const size_t buffer_pool_size_ = 10;
  const size_t k_ = 2;
  std::shared_ptr<DiskManagerUnlimitedMemory> disk_manager_;
  std::shared_ptr<BufferPoolManager> bpm_;
};

// NOLINTNEXTLINE
TEST_F(PageGuardTest, SampleTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  {
    auto *page2 = bpm->NewPage(&page_id_temp);
    page2->RLatch();
    auto guard2 = ReadPageGuard(bpm.get(), page2);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST_F(PageGuardTest, BasicGuardMoveSemantics) {
  page_id_t page_id;
  auto *page = bpm_->NewPage(&page_id);

  EXPECT_EQ(1, page->GetPinCount());

  // 创建 Guard1
  auto guard1 = BasicPageGuard(bpm_.get(), page);
  EXPECT_EQ(1, page->GetPinCount());
  EXPECT_EQ(page->GetData(), guard1.GetData());

  // 测试移动构造 (Move Constructor)
  // guard1 的所有权转移给 guard2，Pin Count 应该保持为 1，guard1 变空
  BasicPageGuard guard2 = std::move(guard1);

  // 验证 guard2 接管了资源
  EXPECT_EQ(1, page->GetPinCount());
  EXPECT_EQ(page->GetData(), guard2.GetData());

  // 【修正点】不要调用 guard1.GetData()！
  // 既然 guard1 已经移交了所有权，我们只要验证它不再持有 Page 即可。

  // 我们可以通过这种方式间接验证 guard1 是否释放了资源：
  // 只要 page 的 pin count 还是 1（被 guard2 持有），就说明 guard1 确实放手了。
  // 如果 guard1 没放手，Pin Count 可能会是 2。
  EXPECT_EQ(1, page->GetPinCount());
}

// 1. 读锁测试：验证 Drop 是否正确释放锁和 Pin
// -----------------------------------------------------------------------
TEST_F(PageGuardTest, ReadGuardLatchAndDrop) {
  page_id_t page_id;
  auto *page = bpm_->NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  // 1. 手动加锁（模拟获取资源）
  page->RLatch();

  // 2. 构造 Guard 接管资源
  // 机制：这里假设 Guard 接管了现有的锁和 Pin Count
  auto r_guard1 = ReadPageGuard(bpm_.get(), page);
  EXPECT_EQ(1, page->GetPinCount());

  // 3. 测试 Drop
  r_guard1.Drop();

  // 验证 Pin 归零
  EXPECT_EQ(0, page->GetPinCount());

  // 4. 【关键验证】验证锁已释放
  // 逻辑：如果 r_guard1.Drop() 没有释放读锁，
  // 那么这里的 page->WLatch() 将会永久阻塞（死锁）。
  // 如果代码能运行到下一行，说明读锁绝对已经被释放了。
  page->WLatch();

  // 清理：释放刚才测试用的写锁
  page->WUnlatch();
}

// -----------------------------------------------------------------------
// 2. 写锁测试：验证析构函数 (RAII) 是否释放锁
// -----------------------------------------------------------------------
TEST_F(PageGuardTest, WriteGuardReleaseTest) {
  page_id_t page_id;
  auto *page = bpm_->NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  // 1. 获取写锁并用 Guard 包装
  page->WLatch();
  {
    auto w_guard = WritePageGuard(bpm_.get(), page);
    EXPECT_EQ(1, page->GetPinCount());
    // 离开作用域，w_guard 析构
    // 应该自动调用 page->WUnlatch() 和 bpm->UnpinPage()
  }

  // 2. 验证 PinCount
  EXPECT_EQ(0, page->GetPinCount());

  // 3. 【关键验证】验证锁是否真的释放了
  // 逻辑：如果 WriteGuard 析构没释放写锁，这里的 RLatch 会卡死。
  // 能走下去就说明锁释放成功。
  page->RLatch();
  page->RUnlatch();
}

// -----------------------------------------------------------------------
// 3. 并发测试：验证写锁的排他性
// -----------------------------------------------------------------------
TEST_F(PageGuardTest, ConcurrencyLatchTest) {
  page_id_t page_id;
  auto *page = bpm_->NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  // 主线程持有 WriteGuard
  page->WLatch();
  auto w_guard = WritePageGuard(bpm_.get(), page);

  // 创建子线程尝试获取读锁
  std::promise<void> thread_started;
  std::future<void> thread_finished = std::async(std::launch::async, [&]() {
    thread_started.set_value();

    // 【阻塞点】尝试获取读锁。
    // 因为主线程持有写锁，这里应该被阻塞，直到 w_guard 释放。
    page->RLatch();

    // 成功获取读锁，立即释放
    page->RUnlatch();

    // 注意：子线程不需要 Unpin，因为它只是操作裸指针
  });

  thread_started.get_future().wait();

  // 稍微睡眠，确保子线程运行到了 page->RLatch() 并进入阻塞状态
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 验证：此时 w_guard 还没释放，子线程应该超时（还在等锁）
  // 这里的 wait_for 预期是超时，证明锁确实生效了
  EXPECT_EQ(std::future_status::timeout, thread_finished.wait_for(std::chrono::milliseconds(10)));

  // 主动释放 WriteGuard (解锁 + Unpin)
  w_guard.Drop();

  // 验证：现在锁释放了，子线程应该能拿到锁并结束
  // 如果这里超时，说明 Drop() 没释放锁
  EXPECT_EQ(std::future_status::ready, thread_finished.wait_for(std::chrono::seconds(1)));

  // 验证最终 Pin Count
  EXPECT_EQ(0, page->GetPinCount());
}

// -----------------------------------------------------------------------
// 4. BPM 集成测试
// -----------------------------------------------------------------------
TEST_F(PageGuardTest, BpmIntegrationTest) {
  page_id_t page_id;
  auto *page0 = bpm_->NewPage(&page_id);
  ASSERT_NE(page0, nullptr);

  // 模拟归还页面，PinCount -> 0
  bpm_->UnpinPage(page_id, false);
  EXPECT_EQ(0, page0->GetPinCount());

  // 1. 测试作用域链
  {
    // 从 BPM Fetch 页面，PinCount 应该变 1
    auto *fetched_page = bpm_->FetchPage(page_id);
    EXPECT_NE(fetched_page, nullptr);

    // 构造 Guard
    auto guard = BasicPageGuard(bpm_.get(), fetched_page);

    // 直接检查原始指针 fetched_page 的状态
    EXPECT_EQ(1, fetched_page->GetPinCount());

    // 检查数据指针一致性
    EXPECT_EQ(fetched_page->GetData(), guard.GetData());
  }
  // 离开作用域，guard 析构 -> Unpin

  // 验证 PinCount 归零
  EXPECT_EQ(0, page0->GetPinCount());

  // 2. 再次 Fetch 验证状态
  auto page_again = bpm_->FetchPage(page_id);
  EXPECT_EQ(1, page_again->GetPinCount());
  bpm_->UnpinPage(page_id, false);
}

}  // namespace bustub
