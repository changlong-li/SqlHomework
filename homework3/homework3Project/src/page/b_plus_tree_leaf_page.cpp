/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <include/page/b_plus_tree_internal_page.h>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id)
{
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetNextPageId(INVALID_PAGE_ID);

    int theSize = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType) - 1;
    SetMaxSize(theSize);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id)
{
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
        const KeyType &key, const KeyComparator &comparator) const {
    int theSize = GetSize();
    for(int i=0; i<theSize; i++)
    {
        if(comparator(key, array[i].first) <= 0)
            return i;
    }
    return theSize;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const
{
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index)
{
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
    if(GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0)
        array[GetSize()] = {key, value};
    else
    {
        int tarIdx = KeyIndex(key, comparator);
        int siz = GetSize();
        for(int i = siz; i > tarIdx; i--)
        {
            array[i] = array[i-1];
//          array[i].first = array[i-1].first;
//          array[i].second = array[i-1].second;
        }
        array[tarIdx] = {key, value};
    }
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
        BPlusTreeLeafPage *recipient,
        __attribute__((unused)) BufferPoolManager *buffer_pool_manager)
{
    int theIdx = (GetMaxSize() + 1) / 2;
    //复制后半部分的键值
    for(int i=theIdx; i < GetMaxSize()+1; i++)
        recipient->array[i - theIdx] = array[i];
    //设置指针
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(recipient->GetPageId());
    //设置和更新Size值
    recipient->SetSize(GetMaxSize() + 1 - theIdx);
    SetSize(theIdx);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size)
{
    for(int i=0; i<size; i++)
    {
        array[i] = *items++;
    }
    IncreaseSize(size);
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
    //这种情况直接输出false
    if(GetSize() == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(GetSize()-1)) > 0)
        return false;

    int tarIdx = KeyIndex(key, comparator);
    if (tarIdx < GetSize() && comparator(array[tarIdx].first, key) == 0)
    {
        value = array[tarIdx].second;
        return true;
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
        const KeyType &key, const KeyComparator &comparator) {
    //此种情况直接返回Size值
    if(GetSize() == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(GetSize()-1)) > 0)
        return GetSize();

    int idx = KeyIndex(key, comparator);
    int theSize = GetSize();
    memmove(array + idx, array + idx + 1, static_cast<size_t>((theSize - idx - 1)*sizeof(MappingType)));
    IncreaseSize(-1);
    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *)
{
    int theSize = GetSize();
    //直接调用其他函数
    recipient->CopyAllFrom(array, theSize);
    recipient->SetNextPageId(GetNextPageId());

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size)
{
    int now = GetSize();
    for(int i=0; i<size; i++)
    {
        array[now + i] = *items++;
    }
    IncreaseSize(size);
}
/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
        BPlusTreeLeafPage *recipient,
        BufferPoolManager *buffer_pool_manager)
{
    MappingType theItem = GetItem(0);
    IncreaseSize(-1);
    //整体向前移动一格
    memmove(array, array + 1, static_cast<size_t>(GetSize()*sizeof(MappingType)));
    recipient->CopyLastFrom(theItem);
    //更新相关的键值
    auto *pg = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(pg->GetData());
    parent->SetKeyAt(parent->ValueIndex(GetPageId()), theItem.first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item)
{
    array[GetSize()] = item;
    IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
        BPlusTreeLeafPage *recipient, int parentIndex,
        BufferPoolManager *buffer_pool_manager)
{
    MappingType pair = GetItem(GetSize() - 1);
    IncreaseSize(-1);
    //直接调用函数
    recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
        const MappingType &item, int parentIndex,
        BufferPoolManager *buffer_pool_manager)
{
    //整体向后移动一格
    memmove(array + 1, array, GetSize()*sizeof(MappingType));
    IncreaseSize(1);
    array[0] = item;

    //更新父母节点键值
    auto *pg = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(pg->GetData());
    parent->SetKeyAt(parentIndex, item.first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace scudb
