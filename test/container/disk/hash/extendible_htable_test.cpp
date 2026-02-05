//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_test.cpp
//
// Identification: test/container/disk/hash/extendible_htable_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"
#include "storage/disk/disk_manager_memory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 2);

  int num_keys = 8;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // attempt another insert, this should fail because table is full
  ASSERT_FALSE(ht.Insert(num_keys, num_keys));
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, RemoveTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // remove the keys we inserted
  for (int i = 0; i < num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_TRUE(removed);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // try to remove some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_FALSE(removed);
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

// 专注于测试分裂和目录扩展
// Bucket Size = 2, 迫使频繁 Split
TEST(ExtendibleHTableTest, DirectoryGrowthTest) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  // header_max_depth=0 (直接映射到目录), directory_max_depth=9, bucket_max_size=2
  DiskExtendibleHashTable<int, int, IntComparator> ht("growth_test", bpm.get(), IntComparator(), HashFunction<int>(), 0,
                                                      9, 2);

  int num_keys = 100;

  // 1. 插入数据，触发多次分裂
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);

    // 每次插入后立即验证，确保刚插进去的能读出来
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // 2. 验证所有数据依然存在（防止分裂过程中丢数据）
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  // 3. 尝试插入重复键，应失败
  ASSERT_FALSE(ht.Insert(0, 100));
}

// 专注于测试合并和缩容
TEST(ExtendibleHTableTest, RecursiveMergeTest) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  // bucket_max_size=2
  DiskExtendibleHashTable<int, int, IntComparator> ht("merge_test", bpm.get(), IntComparator(), HashFunction<int>(), 0,
                                                      8, 2);

  int num_keys = 50;

  // 1. 填满数据
  for (int i = 0; i < num_keys; i++) {
    bool b = ht.Insert(i, i);
    if (!b) {
      std::cout << "插入失败" << std::endl;
    }
    ASSERT_TRUE(b);
  }
  ht.VerifyIntegrity();

  // 2. 逐步删除数据
  for (int i = 0; i < num_keys; i++) {
    ASSERT_TRUE(ht.Remove(i));

    // 确保删除了就真的查不到了
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());

    // 可选：在这里调用 VerifyIntegrity() 可以检测每一步合并是否破坏了结构
    // ht.VerifyIntegrity();
  }

  ht.VerifyIntegrity();

  // 3. 验证此时表是空的
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    ASSERT_FALSE(ht.GetValue(i, &res));
  }
}

// 极小的 Buffer Pool，强制页面置换 (Unpin/Fetch)
TEST(ExtendibleHTableTest, SmallBufferPoolTest) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  // 关键点：BPM Size 只有 3！
  auto bpm = std::make_unique<BufferPoolManager>(4, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("eviction_test", bpm.get(), IntComparator(), HashFunction<int>(),
                                                      0, 8, 10);

  int num_keys = 100;

  // 1. 插入大量数据，迫使 BPM 疯狂换页
  for (int i = 0; i < num_keys; i++) {
    bool b = ht.Insert(i, i);
    if (!b) {
      std::cout << i << "插入失败" << std::endl;
    }
    ASSERT_TRUE(b);
  }
  ht.VerifyIntegrity();

  // 2. 再次读取所有数据，此时必须从 Disk 重新 Fetch 页面
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value) << "Failed to retrieve key " << i;
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  // 3. 删除一半数据
  for (int i = 0; i < num_keys / 2; i++) {
    ASSERT_TRUE(ht.Remove(i));
  }
  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, MixedRandomTest) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("random_test", bpm.get(), IntComparator(), HashFunction<int>(), 0,
                                                      8, 5);

  int num_ops = 1000;
  std::vector<int> keys;

  for (int i = 0; i < num_ops; i++) {
    int op = rand() % 3;     // 0: Insert, 1: Remove, 2: Get
    int key = rand() % 100;  // Key 范围 0-99

    if (op == 0) {
      // Insert
      bool status = ht.Insert(key, key);
      if (status) {
        keys.push_back(key);
      }
    } else if (op == 1) {
      // Remove
      // 如果我们知道这个key之前插入过，status 应该是 true (除非已经被删了)
    } else {
      // Get
      std::vector<int> res;
      ht.GetValue(key, &res);
      // 这里很难断言，因为我们不知道 key 此时是否应该存在，
      // 但我们至少保证程序不会 Crash
    }
  }
  ht.VerifyIntegrity();
}

}  // namespace bustub
