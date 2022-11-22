#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
using namespace std;
namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */

// 默认构造函数的具体实现
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() {
    globalDepth = 0;
    bucketVolume = 32;
    bucketCount = 1;
    bucketTable.push_back(make_shared<Bucket>(0));
}

// 重载构造函数的具体实现
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
    globalDepth = 0;
    bucketVolume = size;
    bucketCount = 1;
    bucketTable.push_back(make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
// 计算输入键值哈希地址的具体实现
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) const{
  return hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    lock_guard<mutex> lock(myLatch); //对临界资源上锁，防止线程之间出现竞争死锁
    return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    if(bucketTable[bucket_id]){ //首先判断该bucket_id是否存在
        lock_guard<mutex> lock(bucketTable[bucket_id]->latch); // 加锁，防止并发错误
        if (bucketTable[bucket_id]->kvmap.size() == 0) //若存在，判断这个bucket中是否有元素
            return -1;
        return bucketTable[bucket_id]->localDepth;
    }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    lock_guard<mutex> lock(myLatch); // 加锁，防止并发错误
    return bucketCount;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    int bucket_id = getBucketId(key); //根据键得到bucket的id
    lock_guard<mutex> lock(bucketTable[bucket_id]->latch); // 加锁，防止并发错误
    shared_ptr<Bucket> tmpPtr = bucketTable[bucket_id];

    //判断是否在这个bucket里找到这个键key的元素
    if (tmpPtr->kvmap.find(key) != tmpPtr->kvmap.end()) {
        value = tmpPtr->kvmap[key]; //通过引用传值
        return true; //表示成功找到

    }
    return false; //表示没找到
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
    int bucket_id = getBucketId(key); //得到bucket的id
    shared_ptr<Bucket> tmpPtr = bucketTable[bucket_id];
    lock_guard<mutex> lock(tmpPtr->latch); //加锁，防止并发错误
    if (tmpPtr->kvmap.find(key) == tmpPtr->kvmap.end()) { //判断该键的元素是否存在
        return false;
    }
    tmpPtr->kvmap.erase(key); //若找到，删除该元素
    return true;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    int bucket_id = getBucketId(key); //获得bucket的id
    shared_ptr<Bucket> tmpPtr = bucketTable[bucket_id]; //使用临时指针代替
    //更新需要更新的bucket和bucket表
    while (true) {
        lock_guard<mutex> lock1(tmpPtr->latch); //加锁，防止并发错误
        //找到是键是key的元素或者bucket还没满
        if (tmpPtr->kvmap.find(key) != tmpPtr->kvmap.end() || tmpPtr->kvmap.size() < bucketVolume) {
            tmpPtr->kvmap[key] = value; //写入元素
            break;
        }
        int mask = (1 << (tmpPtr->localDepth));
        tmpPtr->localDepth++;

        {
            lock_guard<mutex> lock2(myLatch);
            if (tmpPtr->localDepth > globalDepth) {

                size_t length = bucketTable.size();
                for (size_t i = 0; i < length; i++) { //此时把bucket地址的表扩大一倍
                    bucketTable.push_back(bucketTable[i]);
                }
                globalDepth++;

            }
            bucketCount++;
            auto newBuc = make_shared<Bucket>(tmpPtr->localDepth); //创建新bucket

            //为新bucket装元素，同时删掉以前bucket应该删掉的元素
            typename map<K, V>::iterator it;
            for (it = tmpPtr->kvmap.begin(); it != tmpPtr->kvmap.end();) {
                if (HashKey(it->first) & mask) {
                    newBuc->kvmap[it->first] = it->second;
                    it = tmpPtr->kvmap.erase(it);
                } else it++;
            }
            for (size_t i = 0; i < bucketTable.size(); i++) {
                if (bucketTable[i] == tmpPtr && (i & mask)) //把表中的指针指向新bucket
                    bucketTable[i] = newBuc;
            }
        }

        bucket_id = getBucketId(key);
        tmpPtr = bucketTable[bucket_id];
    }
}


//根据键key计算bucket的id的函数
template <typename K, typename V>
int ExtendibleHash<K, V>::getBucketId(const K &key) const{
    lock_guard<mutex> lock(myLatch); //加锁，防止并发错误
    return HashKey(key) & ((1 << globalDepth) - 1); //返回计算出的key对应的bucket的id
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb
