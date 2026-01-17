#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.

  root_lock_.lock();
  // 如果Put和Remove正在将旧树修改为新树的瞬间，获取了old_Trie的话，那么就可能得到修改了一般的root_
  // 因此，可以得知：
  // root_lock_的作用是确保获取root时不会有任何其他进程修改root
  // 或者修改root时不会有任何其他进程也在修改root
  auto old_trie = root_;
  root_lock_.unlock();

  auto value = old_trie.Get<T>(key);
  const auto &final_val = *value;

  if (value == nullptr) {
    return std::nullopt;
  }
  return std::make_optional<ValueGuard<T>>(old_trie, final_val);
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.

  // 主要分三个阶段：1.拿到旧树  2.对旧树修改  3.将旧树修改为新树
  // 哪些需要write_lock_保护
  // 首先，两个并发的Put不能拿到同一个旧树然后进行修改，否则，它们之间会展开竞速，后完成的那个会将旧树修改为对应的新树，先完成的改动被覆盖
  // 因此，所有过程都需要write_lock保护
  // 如果不等到旧树被彻底修改为新树就获取root，那么其他进程就始终能够获取到旧树然后进行修改

  write_lock_.lock();

  root_lock_.lock();
  auto old_trie = root_;
  root_lock_.unlock();

  const auto &new_trie = old_trie.Put<T>(key, std::move(value));

  root_lock_.lock();
  root_ = std::move(new_trie);
  root_lock_.unlock();

  write_lock_.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  write_lock_.lock();

  root_lock_.lock();
  auto old_trie = root_;
  root_lock_.unlock();

  const auto &new_trie = old_trie.Remove(key);

  root_lock_.lock();
  root_ = new_trie;
  root_lock_.unlock();

  write_lock_.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
