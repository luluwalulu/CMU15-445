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

  header_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  result->clear();
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
  // 如果对应的桶尚未被初始化
  if(bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(direc_page, bucket_idx, key, value);
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  if(!bucket_page){
    return false;
  }

  // 这里插入失败说明是对应的桶已满，应该进行分裂
  while(bucket_page->IsFull()){
    auto old_lodep = direc_page->GetLocalDepth(bucket_idx);
    auto old_glodep = direc_page->GetGlobalDepth();
    if(old_lodep == old_glodep) {
      // 需要扩大directory
      if(old_glodep == directory_max_depth_) {
        return false;
      }
      direc_page->IncrGlobalDepth();
    }
    
    // 增加全局深度后归一到所有桶局部深度小于全局深度，且一个桶已满需要分裂的情况

    // 获取一个新的桶页，并将旧桶中的数据分到这两个桶中
    bucket_idx = direc_page->HashToBucketIndex(hash);
    page_id_t new_bucket_page_id;
    auto b_new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
    if(new_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    auto new_bucket_guard = b_new_bucket_guard.UpgradeWrite();
    auto new_bucket_page = new_bucket_guard.template AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
    new_bucket_page->Init(bucket_max_size_);

    auto local_depth = direc_page->GetLocalDepth(bucket_idx);
    auto new_mask = ((1<<(local_depth+1))-1);

    auto new_tidx = bucket_idx & new_mask;
    
    auto size = bucket_page->Size();
    for(size_t i = 0; i < size; i++) {
      auto key = bucket_page->KeyAt(i);
      auto value = bucket_page->ValueAt(i);
      auto hash = Hash(key);
      // 应该放入新桶
      if((hash & new_mask) == new_tidx) {
        if(!bucket_page->Remove(key, cmp_)) {
          std::cout<<"Insert fail"<<std::endl;
        }
        if(!new_bucket_page->Insert(key, value, cmp_)) {
          std::cout<<"Insert fail"<<std::endl;
        }
        std::cout<<"key="<<key<<",value="<<value<<"键值对被插入新桶中"<<std::endl;
      }
    }

    UpdateDirectoryMapping(direc_page,bucket_idx,new_bucket_page_id,local_depth+1,new_mask);

    bucket_guard = std::move(new_bucket_guard);
    bucket_page = new_bucket_page;
  }

  bucket_page->Insert(key, value, cmp_);
  return true;
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
  direc_page->Init(directory_max_depth_);

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

  bucket_page->Init(bucket_max_size_);

  // 成功获取bucket_guard和bucket_page和bucket_page_id
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // 选出一个将要被修改过的槽位作为代表，其他槽位参照它决定自己是否需要修改
  // 如果之前和它指向相同的桶，那么需要决定仍然指向自己之前的桶还是指向被修改槽位所指向的新桶

  // 例如以一个满桶作为参照
  // 如果之前和它指向相同的桶，而现在仍应和它指向相同的桶，则修改为指向相同桶
  // 如果之前和它指向相同的桶，而现在不应和它指向相同的桶，则只修改局部深度++

  // 以一个空桶为例
  // 如果之前和它指向相同的桶，则现在一定仍应和它指向相同的桶，相当于始终为上面的第二种情况，只修改局部深度--

  // 判断是否应指向相同的桶，看hash或idx & 局部深度（idx同样是hash的后几位，但是是全局深度位，覆盖局部深度）
  
  // new_bucket_idx:是被修改的槽位
  // new_bucket_page_id:是被修改槽位指向的新桶
  // new_local_depth:是被修改槽位指向新桶的局部深度
  // local_depth_mask:是new_local_depth对应的掩码
  auto old_local_depth = directory->GetLocalDepth(new_bucket_idx);
  uint32_t old_local_depth_mask = ((1<<old_local_depth)-1);
  
  int change = new_local_depth - old_local_depth;

  auto new_tidx = new_bucket_idx & local_depth_mask;
  auto old_tidx = new_bucket_idx & old_local_depth_mask;

  for(size_t idx = 0; idx < directory->Size(); idx++) {
    auto local_depth = directory->GetLocalDepth(idx);
    auto i_mask = ((1<<local_depth)-1);

    // 如果指向相同的旧桶，且仍应指向相同的新桶
    // BUG????
    if ((old_tidx == (idx & i_mask)) && (new_tidx == (idx & local_depth_mask))) {
      directory->SetLocalDepth(idx, new_local_depth);
      directory->SetBucketPageId(idx, new_bucket_page_id);
    } else if (old_tidx == (idx & i_mask)) {
      directory->SetLocalDepth(idx,local_depth+change);
    }
  }                                                              
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // 删除时只需要直接将当前空桶指向它的分裂桶即可
  // 即使分裂桶为空，只需要循环处理就行了
  auto hash = Hash(key);

  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.template AsMut<ExtendibleHTableHeaderPage>();
  if(!header_page){
    return false;
  }
  auto direc_page_id = header_page->HashToPageId(hash);
  // 如果对应的目录尚未被初始化
  if(direc_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto direc_guard = bpm_->FetchPageWrite(direc_page_id);
  auto direc_page = direc_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  if(!direc_page){
    return false;
  }
  auto bucket_idx = direc_page->HashToBucketIndex(hash);
  auto bucket_page_id = direc_page->HashToPageId(hash);
  // 如果对应的桶尚未被初始化
  if(bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  if(!bucket_page){
    return false;
  }

  // 桶内没有key对应的条目
  if(!bucket_page->Remove(key,cmp_)) {
    return false;
  }

  // 当发现全局深度为0且唯一桶为空时，应该立刻终止
  while(bucket_page->IsEmpty() && !direc_page->GetGlobalDepth()){
    auto split_index = direc_page->GetSplitImageIndex(bucket_idx);
    auto new_local_depth = direc_page->GetLocalDepth(bucket_idx) - 1;
    auto new_mask = ((1<<new_local_depth)-1);

    // 彻底删除旧的桶页
    bucket_guard.Drop();
    bpm_->DeletePage(bucket_page_id);
    UpdateDirectoryMapping(direc_page, bucket_idx, direc_page->GetBucketPageId(split_index), new_local_depth, new_mask);

    // 获取新页
    bucket_idx = split_index;
    bucket_page_id = direc_page->GetBucketPageId(bucket_idx);
    bucket_guard = std::move(bpm_->FetchPageWrite(bucket_page_id));
    bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
    if(!bucket_page) {
      return false;
    }
  }

  if(direc_page->CanShrink()) {
    direc_page->DecrGlobalDepth();
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
