# 当前bug：

- 将锁的问题暂且搁置
- 发现一处错误，之前一旦发现没有水位线可读的版本，待删除列表中一个对象都不移除。
  - 该bug修复后，报错AddressSanitizer: SEGV on unknown address 0x000000000028。原因是垃圾回收中的ModifyUndoLog函数。

---

# 已解决bug：

- 当前bug为垃圾回收器可能删了不该删的东西，导致回退的时候尽管undolink有效，但是GetUndoLog却报错undo log not exist
- **发现错误：我的当前逻辑是只有刚好符合水位线标准的版本才不被删除，剩余都被删除。但是应该只删除水位线以上的更旧的版本**
- **发现错误：最后一个有效版本的undo_link需要置为无效。由于我们并非修改头undo_link，而是修改某个特定版本的undo_log中存储的undo_link，因此我们需要在transaction中进行修改。**

  - 该修改后，undo log not exist的错误仍然存在
  - 且当我试图添加锁时直接报错死锁
- update执行器少检查了一个undolink就直接用这个link来获取log了，导致GetUndoLog挂掉
- 在delete和update执行器中尽可能添加了GetRid，并处理了上面的一条bug，UpdateTest1通过，错误来到UpdateTest2，错误类型为结果不匹配
- **发现错误原因：一个坏消息是，现在的update执行器不必像之前一样先删除再插入了，我们直接在原地更新即可，因此我们不得不堆代码进行大量修改。我们首先对第二个条件的代码进行修改。**
- 当前修改后达到了原地更新的效果，但是结果匹配仍然有误
- 又做了一处修改，发现正常修改逻辑下对于日志中的tuple保存的是新值，但应该是旧值，修复后B测试点有误，应该是自我修复的逻辑有误
- 又做了两处修改，一处是自我更新时temp_schema获取的逻辑有误，另一处和上面一样，日志中保存了新值，但本应保存旧值。修复后当前bug为D测试点的check_undo_log挂掉，但是查询结果正确。
- 之前我判断列相等时可以忽略，元组修改前后相等也能忽略。但是前者可以忽略，后者不能忽略，因为它必须更新这个堆元组的时间戳，以防止发生写写冲突。不幸的是，修复后还是在同样的地方挂逼。
- 其实当前结果符合我的预期，但根据gemini所说，在自我更新合并日志时modified_fileds只能变多不能变少，意味着该列值一旦被修改，即使后面被同一事务改回了相同值，也不能在modified_fileds中改回false。进行相关修改后Task3.4之前的测试通过

* 删除执行器获取seq_scan_executor传输的元组后，发现该元组的Rid为无效rid，该元组当前处于被另一个活跃事务删除的状态，期望结果是发生写写冲突。
* 发现另一个问题是当我们的seq_scan_executor构建出了一个符合要求的read_ts>=commit_ts的元组后，update和delete执行器需要的反而是堆上元组的最新版本，此时我们只应该将rid作为参考物，而忽略child_tuple
* **修改1：我发现在ReconstructTuple函数中，我根据values和schema构建出了最新的tuple之后，没有对这个tuple进行SetRid。修改完该错误之后测试4通过**
* **delete执行器修改2进行后进度和修改1相同，应该是这个测试本身比较拉跨吧。**
* **update执行器修改2进行之后进度还是一样，被其他bug给卡住了**
* **发现一处错误：在删除和更新执行器中，我把TXN_START_ID+transac_id作为了新的时间戳，修改为将transac_id作为临时事务时间戳。修改完后在F: check scan txn5挂掉，原因是结果不匹配，但是明明debug_hook打印结果都正确，结果却不匹配。推测为顺序扫描执行器的问题。**
* 事务2在即txn3在提交时RID0/1未能提交成功，提交后仍然显示txn3。txn2删除值为5的元组，删除时该元组被txn4修改，写写冲突不应生效。
* 所以问题在于txn3提交时未能成功，导致值为2的元组仍然处于未提交状态，因此本不该包含的值为2的元组也被包含在内。
* 发现原因：update执行器和delete执行器进行写操作时没有添加到writesets当中。修复后三号测试通过

- B: check scan txn2时，之后打印版本链时发现撤销日志中有临时事务时间戳。发现是TxnMgrDbg有误
- InsertDeleteTest的txn4结果mismatch- 期待结果两个元组（1，2），结果只有1。同时B: check scan txn2修改之后得debug_hook打印得那个被删除得元组的时间戳是ts=-9223372036854775805，删除元组的时间戳逻辑很可能有误。
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
