#include "buffer/buffer_pool_manager.h"

namespace scudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    lock_guard<mutex> lock(latch_); //加锁，防止并发错误

    Page *target = nullptr;
    //1.1
    if(page_table_->Find(page_id, target)) { //查阅哈希表，此时说明已经在调进来了
        target->pin_count_++;
        replacer_->Erase(target);
    }
    else
    {
        //1.2
        target = findUnusedPage();

        if (target == nullptr) {
            return target;
        }
        //2 若是修改过，写回磁盘
        if (target->is_dirty_) {
            disk_manager_->WritePage(target->GetPageId(),target->data_);
        }
        //3 删除原来的，插入新的
        page_table_->Remove(target->GetPageId());
        page_table_->Insert(page_id,target);
        //4 更新这个page的各种变量参数
        disk_manager_->ReadPage(page_id, target->data_);
        target->page_id_ = page_id;
        target->pin_count_ = 1;
        target->is_dirty_ = false;

    }

    return target; //返回结果
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    lock_guard<mutex> lock(latch_); //加锁，防止并发错误

    Page *p = nullptr;
    //没查到该页的情况
    if (!page_table_->Find(page_id, p) || p->pin_count_ <= 0) {
        return false;
    }

    //对该页执行unPin操作
    p->pin_count_--;
    if (p->pin_count_ == 0) { //若pin_count将降为零，插入可置换列表
        replacer_->Insert(p);
    }
    if (is_dirty) { //更新该页的is_dirty情况
        p->is_dirty_ = true;
    }
    return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    lock_guard<mutex> lock(latch_); //加锁，防止并发错误

    assert(page_id != INVALID_PAGE_ID);
    Page *p = nullptr;
    // 没找到或者id值为INVALID_PAGE_ID的情况
    if (!page_table_->Find(page_id, p) || p->page_id_ == INVALID_PAGE_ID) {
        return false;
    }

    //把is_dirty是真的page写回磁盘，从而让is_dirty属性变为假
    if (p->is_dirty_) {
        disk_manager_->WritePage(page_id, p->GetData());
        p->is_dirty_ = false;
    }

    return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    lock_guard<mutex> lock(latch_); //加锁，防止并发错误
    Page *p = nullptr;
    if (page_table_->Find(page_id, p)) {
        if (p->GetPinCount() > 0) {
            //如果该页在使用，不能删除
            return false;
        }
        // 重置该页
        p->page_id_ = INVALID_PAGE_ID;
        p->is_dirty_ = false;
        p->pin_count_ = 0;
        p->ResetMemory();

        //各种表中删除这个page
        replacer_->Erase(p);
        page_table_->Remove(page_id);
        free_list_->push_back(p);
    }
    disk_manager_->DeallocatePage(page_id);

    return true;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    lock_guard<mutex> lock(latch_); //加锁，防止并发错误

    Page *newPg = nullptr;
    newPg = findUnusedPage();

    if (newPg== nullptr) {
        return newPg;
    }

    page_id = disk_manager_->AllocatePage();
    //若这个page是dirty，写回磁盘，然后清空这个page
    if (newPg->is_dirty_) {
        disk_manager_->WritePage(newPg->GetPageId(),newPg->data_);
    }
    page_table_->Remove(newPg->GetPageId());

    // 初始化参数
    newPg->page_id_ = page_id;
    newPg->is_dirty_ = false;
    newPg->pin_count_ = 1;

    //新page插入表中
    page_table_->Insert(newPg->page_id_, newPg);

    return newPg;
}

Page *BufferPoolManager::findUnusedPage() {
    Page *p = nullptr;
    if (!free_list_->empty()) {

        //选中可用的page
        p = free_list_->front();
        free_list_->pop_front(); //free list弹出一个


        assert(p->page_id_ == INVALID_PAGE_ID);
        assert(p->pin_count_ == 0);
        assert(!p->is_dirty_);
    }
    else
    {
        // 空闲和可替换的都没有的情况下
        if (!replacer_->Victim(p)) {
            return nullptr;
        }

        //在表中删除选中的page
        page_table_->Remove(p->page_id_);
        //需要的时候写回磁盘
        if (p->is_dirty_) {
            disk_manager_->WritePage(p->page_id_, p->GetData());
            p->is_dirty_ = false;
        }
        //重置存储空间
        p->ResetMemory();
        p->page_id_ = INVALID_PAGE_ID;
    }
    return p;
}


} // namespace scudb
