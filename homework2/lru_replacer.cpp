/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
    lock_guard<mutex> lock(latch); //加锁，防止并发错误

    //若value已经存在，直接先删掉，若不存在，这一步相当于不做操作，统一后面的操作
    erase(value);

    auto tar = make_shared<LinkedNode>(value);

    if (tar == nullptr) {
        return;
    }
    //链表指针的调换
    tar->pre = nullptr;
    tar->next = head;

    if (head != nullptr) {
        head->pre = tar;
    }
    head = tar;
    if (tail == nullptr) {
        tail = tar;
    }

    mp[tar->value] = tar;
    siz++;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    lock_guard<mutex> lock(latch); //加锁，防止并发错误

    if (siz == 0) {
        return false;
    }
    if (head == tail) { //只有一个元素的情况
        value = head->value;
        head = nullptr;
        tail = nullptr;

        mp.erase(value);
        siz--;
        return true;
    }
    //有两个及以上元素时
    value = tail->value;
    auto tmp = tail;
    tmp->pre->next = nullptr;
    tail = tmp->pre;
    tmp->pre = nullptr;

    mp.erase(value);
    siz--;
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
    lock_guard<mutex> lock(latch); //加锁，防止并发错误

    auto iter = mp.find(value);
    if (iter == mp.end()) { //没找到的时候
        return false;
    }

    //调用自定义的封装的删除函数来删除
    return erase(value);
}

template <typename T> size_t LRUReplacer<T>::Size() {
    lock_guard<mutex> lock(latch); //加锁，防止并发错误
    return siz;
}

//封装自定义的删除函数，方便Insert函数和Erase函数在具体实现中调用
template <typename T> bool LRUReplacer<T>::erase(const T &value) {
    auto iter = mp.find(value);
    if (iter == mp.end()) {
        return false;
    }

    //分为多种情况讨论
    auto ptr = iter->second;
    if (ptr == head && ptr == tail) {
        head = nullptr;
        tail = nullptr;
    } else if (ptr == head) {
        ptr->next->pre = nullptr;
        head = ptr->next;
    } else if (ptr == tail) {
        ptr->pre->next = nullptr;
        tail = ptr->pre;
    } else {
        ptr->pre->next = ptr->next;
        ptr->next->pre = ptr->pre;
    }
    ptr->pre = nullptr;
    ptr->next = nullptr;

    mp.erase(value);
    siz--;
    return true;
}


template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb
