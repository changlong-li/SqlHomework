/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <mutex>
#include <map>
#include <memory>
#include "hash/hash_table.h"
using namespace std;

namespace scudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
    //用于表示buckets定义的结构体
    struct Bucket {
        Bucket(int depth) : localDepth(depth) {};
        int localDepth; //表示每个bucket的容量，是个固定值
        map<K, V> kvmap;
        mutex latch;
    };
public:
  // constructor
  ExtendibleHash(); //默认构造函数，没有传入参数时，按默认参数初始化bucket
  ExtendibleHash(size_t size); //重载构造函数，主要用于初始化bucket
  // helper function to generate hash addressing
  size_t HashKey(const K &key) const;
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

  int getBucketId(const K &key) const;

private:
  // add your own member variables here
  int globalDepth;
  size_t bucketVolume; //bucket容量
  int bucketCount; //bucket数量
  vector<shared_ptr<Bucket>> bucketTable; //使用vectot存储所有bucket
  mutable mutex myLatch; //锁变量
};

} // namespace scudb
