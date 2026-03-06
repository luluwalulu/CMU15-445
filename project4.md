# 当前bug：

- InsertDeleteTest的txn4结果mismatch
  - 期待结果两个元组（1，2），结果只有1

---

# 已解决bug：

- 在delete_executor中，当我尝试获取当前堆元组的undolink时，没有考虑undolink不存在，或者undolink无效。可能会出现当前事务刚刚插入该元组，该元组尚未有版本链，所以这样的特殊情况是完全可能存在的。文档中直接提及“当一个事务插入元组，进行多次修改然后将其删除的情况，可以直接修改表堆元组无需生成任何撤销日志”。问题就变得很清晰了。修复后发现死循环，因为把delete执行器的is_finished逻辑误删了。

- 修复上一处bug后发现扫描的元组数量大于期待的数量
  问题应该出在Insert算子上，插入后的元组好像并不清除其对应事务的时间戳
  发现问题在于对于临时事务时间戳理解有误，临时事务时间戳就是事务ID
  因此在顺序扫描执行器和插入执行器中调整了相关逻辑
- 修复了seq_scan_executor中，有关是否需要回退的逻辑判断，临时事务时间戳应该是
  TXN_START_ID + 当前事务的读时间戳

- A: check scan txn1中结果不符，在堆中元组处于修改状态，且该修改状态由当前事务提交时，
  没有返回堆中元组，反而打印该元组回退失败，进入了回退逻辑

- 打印错误消息"undo log not exist"
  发现是TxnDbgMgr函数对于回退的判断逻辑只关注了undo_link->IsValid()，
  没有关注undo_link.has_value()

- 当前bug为打印时会打印不该打印的东西
  读时间戳为0时明明应该什么都看不到，却打印了四个元组
  修复两处bug，一处是元组回退失败后迭代器没有正常++
  另一处是回退成功的条件匹配有误
  修复后Task2.2通过

- 打印错误消息Message :: undo log not exist
  因为在处理undo_link时没有考虑prev_txn_==INVALID_TXN_ID的情况
  修复后打印错误结果
  发现一处错误，t = *ReconstructTuple(&GetOutputSchema(), t, meta, undo_logs);
  这里重构函数要求提供的是元组的最开始的版本，如果提供t的话t会不断的变化

- C2: verify 2nd record
  tuple data mismatch: got (0, 1.000000, `<NULL>`); at column 1, actual value 1.000000 != expected value 2.000000
  本来的判断应该是当元组操作前后都处于被删除状态则跳过，但是条件误写为!deleted && !deleted
  修改后测试通过

- 当前bug为，水位线本该为5，但却为4
  猜测还是RemoveTxn中的while逻辑有误。
  假设读时间戳为2的事务长时间未能完成，而时间戳为3，4，5的事务已经完成并提交
  按照之前的逻辑来讲，current_reads在移除了txn2之后应该为空，循环立刻终止，但watermark_没有更新
  所以，对于移除事务后导致水位线需要下降，且下降后导致current_reads为空的情况，
  我们直接将watermark更新为commit_ts

- RemoveTxn函数中，如果while的循环条件只有current_reads_.find(watermark_) == current_reads_.end()的话
  ， 那么假如watermark之下的所有事务都已经完成，那么将会无限循环。
  因此添加额外判断条件current_reads是否为空
