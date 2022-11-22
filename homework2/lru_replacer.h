/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <memory>
#include <unordered_map>
#include <mutex>
#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

namespace scudb {

template <typename T> class LRUReplacer : public Replacer<T> {
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
  // add your member variables here
  struct LinkedNode { //定义自己需要使用的结构体
      LinkedNode(T v) :value(v) {
      }

      T value;
      shared_ptr<LinkedNode> pre;
      shared_ptr<LinkedNode> next;
  };

  bool erase(const T &value); //自定义一个在链表中删除元素的函数，方便Insert和Erase函数调用

  //head和tail在链表里是浮动的，空或指向链表中的某个元素
  shared_ptr<LinkedNode> head;
  shared_ptr<LinkedNode> tail;
  int siz = 0; //记录元素个数
  mutable mutex latch;
  map<T, shared_ptr<LinkedNode>> mp; //value与地址相对应的字典
};

} // namespace scudb
