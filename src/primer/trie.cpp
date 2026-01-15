#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  std::shared_ptr<const TrieNode> p(root_);
  for(int i=0;i<key.size();i++){
    if(p->children_.find(key[i])==p->children_.end()) return nullptr;
    p=p->children_[key[i]];
  }
  if(!p->is_value_node_) return nullptr;
  const TrieNodeWithValue<T>* final = dynamic_cast<const TrieNodeWithValue<T>*> (p);
  if(final==nullptr) return nullptr;
  return final.get();
}

template <class T>
auto Trie::put_reversal(std::shared_ptr<TrieNode> p,int index,std::string_view key,T val) const -> std::shared_ptr<const TrieNode> {
  // 插入完成的终止条件
  if(index==key.size()){

  }
  std::shared_ptr<TrieNode> next;
  // 如果p的孩子中存在序号为index的对应节点
  if(p->children_.find(key[index])!=p->children_.end())
    next=std::shared_ptr<TrieNode>(std::move(p->children_[key[index]]->Clone()));
  else
    next=std::make_shared<TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>());

  p->children_[key[index]]=put_reversal(next,index+1,key,val);

  return p; 
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> newroot,p;
  newroot=std::shared_ptr<TrieNode>(std::move(root_->Clone()));
  p=newroot;
  for(int i=0;i<key.size()-1;i++){
    std::shared_ptr<TrieNode> temp;
    if(p->children_.find(key[i])==p->children_.end()){
      temp=std::make_shared<TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>());
      p->children_[key[i]]=std::shared_ptr<const TrieNode>(temp);
    }
    else{
      temp=std::shared_ptr<TrieNode>(std::move(p->children_[key[i]]->Clone()));
      p->children_[key[i]]=std::shared_ptr<const TrieNode>(temp);
    }
    p=temp;
  }


}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
