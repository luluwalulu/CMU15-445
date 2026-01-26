//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  page_id_t page_id;
  auto guard = bpm_->NewPageGuarded(&page_id);
  // 我们只需要持久化page_id，并不需要始终让header_page存在于缓冲池中
  header_page_id_ = page_id;

  auto header_page = guard.AsMut<ExtendibleHTableHeaderPage>();

  header_page->Init();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<ExtendibleHTableHeaderPage>();
  if(!header_page){
    return false;
  }

  // 根据hash值，先得到index，再通过index在数组中查询页号
  auto hash = Hash(key);
  auto direc_page_id = header_page->HashToPageId(hash);
  if(direc_page_id == INVALID_PAGE_ID){
    return false;
  }

  auto direc_guard = bpm_->FetchPageRead(direc_page_id);
  auto direc_page = direc_guard.template As<ExtendibleHTableDirectoryPage>();
  if(!direc_page){
    return false;
  }

  // 根据hash值，先得到index，再通过index在数组中查询页号
  auto bucket_page_id = direc_page->HashToPageId(hash);

  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  if(!bucket_page){
    return false;
  }

  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = Hash(key);

  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.template AsMut<ExtendibleHTableHeaderPage>();
  if(!header_page){
    return false;
  }
  auto direc_idx = header_page->HashToDirectoryIndex(hash);
  auto direc_page_id = header_page->HashToPageId(hash);
  // 如果对应的目录尚未被初始化
  if(direc_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, direc_idx, hash, key, value);
  }

  auto direc_guard = bpm_->FetchPageWrite(direc_page_id);
  auto direc_page = direc_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  if(!direc_page){
    return false;
  }
  auto bucket_idx = direc_page->HashToBucketIndex(hash);
  auto bucket_page_id = direc_page->HashToPageId(hash);

  if(bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(direc_page, bucket_idx, key, value);
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  if(!bucket_page){
    return false;
  }
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t direc_page_id;
  auto b_direc_guard = bpm_->NewPageGuarded(&direc_page_id);
  if(direc_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto direc_guard = b_direc_guard.UpgradeWrite();
  auto direc_page = direc_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  header->SetDirectoryPageId(directory_idx,direc_page_id);

  // 成功获取direc_guard和direc_page和direc_page_id
  direc_page->Init();

  auto bucket_idx = direc_page->HashToBucketIndex(hash);
  return InsertToNewBucket(direc_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  auto b_bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  if(bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = b_bucket_guard.UpgradeWrite();
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  directory->SetBucketPageId(bucket_idx,bucket_page_id);

  // 成功获取bucket_guard和bucket_page和bucket_page_id
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
