/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetParentPageId(parent_id);
    SetPageId(page_id);

    SetPageType(IndexPageType::INTERNAL_PAGE);

    int max_size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1;
    SetMaxSize(max_size);
    SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    int len = GetSize();
    for(int i = 0; i < len; i++)
    {
        if(value != ValueAt(i))
            continue;
        else
            return i;
    }
    return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const
{
    assert(index >= 0 && index < GetSize());
    auto res = array[index].second;
    return res;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    assert(GetSize() > 1);
    int l = 1, r = GetSize() - 1;
    while(l <= r)
    {
        int mid = (r - l) / 2 + l;
        if(comparator(array[mid].first, key) <= 0)
            l = mid + 1;
        else
            r = mid - 1;
    }
    return array[l-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
        const ValueType &old_value, const KeyType &new_key,
        const ValueType &new_value) {
    array[1] = {new_key, new_value};
//    array[1].first = new_key;
//    array[1].second = new_value;
    array[0].second = old_value;
    SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
        const ValueType &old_value, const KeyType &new_key,
        const ValueType &new_value) {
    int now = ValueIndex(old_value) + 1;

    assert(now > 0);
    IncreaseSize(1);
    int nowSize = GetSize();

    for(int i = nowSize - 1; i > now; i--)
    {
        array[i] = array[i-1];
//      array[i].first = array[i - 1].first;
//      array[i].second = array[i - 1].second;
    }

    array[now] = {new_key, new_value};
//  array[now].first = new_key;
//  array[now].second = new_value;

    return nowSize;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
        BPlusTreeInternalPage *recipient,
        BufferPoolManager *buffer_pool_manager) {
    assert(recipient != nullptr);
    page_id_t newPageId = recipient->GetPageId();

    int oldSize = GetMaxSize() + 1;
    int now = oldSize / 2;

    for(int i = now; i < oldSize; i++)
    {
        recipient->array[i - now] = array[i];
//        recipient->array[i - now].first = array[i].first;
//        recipient->array[i - now].second = array[i].second;

        auto thisOLdPage = buffer_pool_manager->FetchPage(array[i].second);
        BPlusTreePage *thisNewPage = reinterpret_cast<BPlusTreePage *>(thisOLdPage->GetData());
        thisNewPage->SetParentPageId(newPageId);
        buffer_pool_manager->UnpinPage(array[i].second, true);
    }

    SetSize(now);
    recipient->SetSize(oldSize - now);
}
///
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
        MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
    for (int i = 0; i < size; ++i)
        array[i] = *items++;
    IncreaseSize(size);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    assert(index >= 0 && index < GetSize());
    int pageSize = GetSize();

    for(int i = index + 1; i < pageSize; i++)
        array[i-1] = array[i];

    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType res = ValueAt(0);
  IncreaseSize(-1);
  assert(GetSize() == 0);
  return res;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
        BPlusTreeInternalPage *recipient, int index_in_parent,
        BufferPoolManager *buffer_pool_manager){
    int start = recipient->GetSize();
    page_id_t recipPageId = recipient->GetPageId();
    // 首先找到父母节点
    Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
    assert(page != nullptr);
    BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    SetKeyAt(0, parent->KeyAt(index_in_parent));
    buffer_pool_manager->UnpinPage(parent->GetPageId(), false);

    //把键值复制到新节点
    for (int i = 0; i < GetSize(); ++i) {
        recipient->array[start + i] = array[i];
//        recipient->array[start + i].first = array[i].first;
//        recipient->array[start + i].second = array[i].second;
        //更新子节点的父母节点
        auto childRawPage = buffer_pool_manager->FetchPage(array[i].second);
        BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
        childTreePage->SetParentPageId(recipPageId);
        buffer_pool_manager->UnpinPage(array[i].second,true);
    }
    //更新Size值
    recipient->SetSize(start + GetSize());
    assert(recipient->GetSize() <= GetMaxSize());
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
        MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    int st = GetSize();
    for(int i=0; i < size; i++)
        array[i + st] = *items++;
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
        BPlusTreeInternalPage *recipient,
        BufferPoolManager *buffer_pool_manager) {

    MappingType pair{KeyAt(0), ValueAt(0)};
    IncreaseSize(-1);

    int thisSize = GetSize();
    memmove(array, array + 1, static_cast<size_t>(thisSize * sizeof(MappingType)));
    recipient->CopyLastFrom(pair, buffer_pool_manager);

    //更新page id
    page_id_t cPageId = pair.second;
    Page *pg = buffer_pool_manager->FetchPage(cPageId);
    BPlusTreePage *childPtr = reinterpret_cast<BPlusTreePage *>(pg->GetData());
    childPtr->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(childPtr->GetPageId(), true);

    //更新相关的键值
    pg = buffer_pool_manager->FetchPage(GetParentPageId());
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(pg->GetData());
    parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
        const MappingType &pair, BufferPoolManager *buffer_pool_manager)
{
    assert(GetSize() + 1 <= GetMaxSize());
    int theSize = GetSize();
    array[theSize] = pair;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
        BPlusTreeInternalPage *recipient, int parent_index,
        BufferPoolManager *buffer_pool_manager)
{
    MappingType tmp = array[GetSize() - 1];
    IncreaseSize(-1);
    recipient->CopyFirstFrom(tmp, parent_index, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
        const MappingType &pair, int parent_index,
        BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() + 1 < GetMaxSize());
    //整体向后移动一个单位
    memmove(array + 1, array, GetSize()*sizeof(MappingType));
    IncreaseSize(1);
    array[0] = pair;
    // 更新page id
    page_id_t childPid = pair.second;
    Page *pg = buffer_pool_manager->FetchPage(childPid);
    assert (pg != nullptr);
    BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(pg->GetData());
    child->SetParentPageId(GetPageId());
    assert(child->GetParentPageId() == GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    //更新相关键值
    pg = buffer_pool_manager->FetchPage(GetParentPageId());
    B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(pg->GetData());
    parent->SetKeyAt(parent_index, pair.first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace scudb
