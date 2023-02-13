/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace scudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          page_id_t root_page_id)
        : index_name_(name), root_page_id_(root_page_id),
          buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const
{
    return root_page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::rootLockedCnt = 0;
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    //首先，找到目标页
    auto *this_leaf = FindLeafPage(key, false, OpType::READ, transaction);
    if(this_leaf == nullptr) return false;
    //然后，找到目标值
    result.resize(1);
    auto state = this_leaf->Lookup(key, result[0], comparator_);
    //后续收尾工作
    FreePagesInTransaction(false, transaction, this_leaf->GetPageId());
    return state;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    LockRootPageId(true);
    //如果是空，直接新建
    if(IsEmpty())
    {
        StartNewTree(key, value);
        TryUnlockRootPageId(true);
        return true;
    }
    TryUnlockRootPageId(true);
    //此时直接调用函数
    return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value)
{
    //首先，从buffer池里找新页
    Page *page = buffer_pool_manager_->NewPage(root_page_id_);
    auto root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    //然后，更新树的根page id
    UpdateRootPageId(true);
    root->Init(root_page_id_, INVALID_PAGE_ID);
    //最后，插入键值
    root->Insert(key, value, comparator_);
    //后续收尾工作
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    auto *leaf = FindLeafPage(key, false, OpType::INSERT, transaction);
    if(leaf == nullptr) return false;
    ValueType v;
    //如果已存在，就不插入了
    if(leaf->Lookup(key, v, comparator_))
    {
        FreePagesInTransaction(true, transaction);
        return false;
    }

    leaf->Insert(key, value, comparator_);
    //此时需要分裂节点
    if(leaf->GetSize() > leaf->GetMaxSize())
    {
        B_PLUS_TREE_LEAF_PAGE_TYPE *newLeaf = Split(leaf, transaction);
        InsertIntoParent(leaf, newLeaf->KeyAt(0), newLeaf, transaction);
    }
    FreePagesInTransaction(true, transaction);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction)
{
    //首先，在buffer池中找新页
    page_id_t page_id;
    Page* const page = buffer_pool_manager_->NewPage(page_id);
    page->WLatch();
    transaction->AddIntoPageSet(page);
    //然后把一半的键值对移动到新页里
    auto new_node = reinterpret_cast<N *>(page->GetData());
    new_node->Init(page_id);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    //返回新节点
    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction)
{
    //当老节点是根节点时
    if(old_node->IsRootPage())
    {
        //找新页
        auto *page = buffer_pool_manager_->NewPage(root_page_id_);
        auto root = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
        root->Init(root_page_id_);
        root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        //更新父母节点page id
        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);
        //更新根节点page id
        UpdateRootPageId();

        buffer_pool_manager_->UnpinPage(root->GetPageId(), true);

    }
    else
    {
        //否则，按一般情况处理
        page_id_t parent_id = old_node->GetParentPageId();
        auto *page = FetchPage(parent_id);
        auto *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
        new_node->SetParentPageId(parent_id);
        //把新节点插入旧节点后面
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        if(parent->GetSize() > parent->GetMaxSize())
        {
            auto *new_leaf = Split(parent, transaction);
            InsertIntoParent(parent, new_leaf->KeyAt(0), new_leaf, transaction);
        }
        buffer_pool_manager_->UnpinPage(parent_id, true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction)
{
    if(IsEmpty())
        return;
    auto *leaf = FindLeafPage(key, false, OpType::DELETE, transaction);
    int nowSize = leaf->RemoveAndDeleteRecord(key, comparator_);
    if(nowSize < leaf->GetMinSize()) CoalesceOrRedistribute(leaf, transaction);
    FreePagesInTransaction(true, transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    //当node是根节点时
    if (node->IsRootPage())
    {
        //调用根节点调整函数
        bool old_root = AdjustRoot(node);
        if (old_root) {transaction->AddIntoDeletedPageSet(node->GetPageId());}
        return old_root;
    }
    //一般情况时，先找兄弟节点和父母节点
    N *node2;
    bool isRightSib = FindLeftSibling(node,node2,transaction);
    BPlusTreePage *parent = FetchPage(node->GetParentPageId());
    B_PLUS_TREE_INTERNAL_PAGE *parentPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parent);
    //此时合并两节点
    if (node->GetSize() + node2->GetSize() <= node->GetMaxSize()) {
        if (isRightSib) {swap(node,node2);} //assumption node is after node2
        int removeIndex = parentPage->ValueIndex(node->GetPageId());
        Coalesce(node2,node,parentPage,removeIndex,transaction);//unpin node,node2
        buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
        return true;
    }
    //此时调用重新分配函数
    int nodeInParentIndex = parentPage->ValueIndex(node->GetPageId());
    Redistribute(node2,node,nodeInParentIndex);//unpin node,node2
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), false);
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindLeftSibling(N *node, N * &sibling, Transaction *transaction) {
    auto page = FetchPage(node->GetParentPageId());
    B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
    int index = parent->ValueIndex(node->GetPageId());
    int siblingIndex = index - 1;
    //此时说明没有左兄弟节点
    if (index == 0) {
        siblingIndex = index + 1;
    }
    sibling = reinterpret_cast<N *>(CrabingProtocalFetchPage(
            parent->ValueAt(siblingIndex),OpType::DELETE,-1,transaction));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    //为真，说明是右兄弟节点
    return index == 0;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
        N *&neighbor_node, N *&node,
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
        int index, Transaction *transaction) {
    //把一个节点上的东西都移动到另一个节点上
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    transaction->AddIntoDeletedPageSet(node->GetPageId());
    parent->Remove(index);
    if (parent->GetSize() <= parent->GetMinSize()) {
        return CoalesceOrRedistribute(parent,transaction);
    }
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) neighbor_node->MoveFirstToEndOf(node,buffer_pool_manager_);
  else neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node)
{
    //当根节点是叶节点时
    if(old_root_node->IsLeafPage())
    {
        if(old_root_node->GetSize() == 0)
        {
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(false);
            return true;
        }
        return false;
    }
    //当根节点size为1时
    if(old_root_node->GetSize() == 1)
    {
        B_PLUS_TREE_INTERNAL_PAGE *root = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
        root_page_id_ = root->RemoveAndReturnOnlyChild();
        UpdateRootPageId();

        auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
        auto new_root = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
        new_root->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        return true;
    }
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin()
{
  KeyType key;
  TryUnlockRootPageId(false);
  return INDEXITERATOR_TYPE(FindLeafPage(key, true), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto *leaf = FindLeafPage(key);
  TryUnlockRootPageId(false);
  int idx = 0;
  if (leaf != nullptr) idx = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf, idx, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost,OpType op,
                                                         Transaction *transaction) {
  bool exclusive = (op != OpType::READ);
  LockRootPageId(exclusive);
  //当为空时
  if (IsEmpty())
  {
    TryUnlockRootPageId(exclusive);
    return nullptr;
  }

  auto ptr = CrabingProtocalFetchPage(root_page_id_, op, -1, transaction);
  page_id_t next;
  for (page_id_t cur = root_page_id_; !ptr->IsLeafPage(); ptr = CrabingProtocalFetchPage(next,op,cur,transaction),cur = next)
  {
    B_PLUS_TREE_INTERNAL_PAGE *internalPage = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(ptr);
    if (leftMost) next = internalPage->ValueAt(0);
    else next = internalPage->Lookup(key,comparator_);
  }

  return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(ptr);
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FetchPage(page_id_t page_id)
{
  auto pg = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(pg->GetData());
}
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::CrabingProtocalFetchPage(page_id_t page_id,OpType op,page_id_t previous, Transaction *transaction) {
  bool exclusive = (op != OpType::READ);
  auto pg = buffer_pool_manager_->FetchPage(page_id);
  Lock(exclusive, pg);

  auto treePage = reinterpret_cast<BPlusTreePage *>(pg->GetData());
  if (previous > 0 && (!exclusive || treePage->IsSafe(op)))
    FreePagesInTransaction(exclusive,transaction,previous);
  if (transaction != nullptr) transaction->AddIntoPageSet(pg);

  return treePage;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePagesInTransaction(bool exclusive, Transaction *transaction, page_id_t cur) {
  TryUnlockRootPageId(exclusive);
  if (transaction == nullptr)
  {
    Unlock(false,cur);
    buffer_pool_manager_->UnpinPage(cur,false);
  }
  else{
      for (Page *pg : *transaction->GetPageSet())
      {
          int pid = pg->GetPageId();
          Unlock(exclusive, pg);
          buffer_pool_manager_->UnpinPage(pid, exclusive);
          if (transaction->GetDeletedPageSet()->find(pid) != transaction->GetDeletedPageSet()->end())
          {
              buffer_pool_manager_->DeletePage(pid);
              transaction->GetDeletedPageSet()->erase(pid);
          }
      }
      transaction->GetPageSet()->clear();
  }
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record)
{
  HeaderPage *hdPage = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));

  if (insert_record) hdPage->InsertRecord(index_name_, root_page_id_);
  else hdPage->UpdateRecord(index_name_, root_page_id_);

  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
  if (IsEmpty()) return "Empty tree";
  std::queue<BPlusTreePage *> todo, tmp;
  std::stringstream tree;
  auto node = reinterpret_cast<BPlusTreePage *>(
          buffer_pool_manager_->FetchPage(root_page_id_));
  if (node == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while printing");
  }
  todo.push(node);
  bool first = true;
  while (!todo.empty()) {
    node = todo.front();
    if (first) {
      first = false;
      tree << "| ";
    }

    if (node->IsLeafPage()) {
      auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
      tree << page->ToString(verbose) <<"("<<node->GetPageId()<< ")| ";
    } else {
      auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
      tree << page->ToString(verbose) <<"("<<node->GetPageId()<< ")| ";
      page->QueueUpChildren(&tmp, buffer_pool_manager_);
    }
    todo.pop();
    if (todo.empty() && !tmp.empty()) {
      todo.swap(tmp);
      tree << '\n';
      first = true;
    }

    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }
  return tree.str();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}


/***************************************************************************
 *  Check integrity of B+ tree data structure.
 ***************************************************************************/

//判断树是否平衡
INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::isBalanced(page_id_t pid) {
  if (IsEmpty()) return true;
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));

  int res = 0;
  if (!node->IsLeafPage())
  {
    auto pg = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(node);
    int last = -2;
    for (int i = 0; i < pg->GetSize(); i++)
    {
      int cur = isBalanced(pg->ValueAt(i));
      if (cur >= 0 && last == -2)
      {
        last = cur;
          res = last + 1;
      }else if (last != cur)
      {
          res = -1;
        break;
      }
    }
  }
  buffer_pool_manager_->UnpinPage(pid,false);
  return res;
}

//判断树是否合法
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::isPageCorr(page_id_t pid,pair<KeyType,KeyType> &out) {
  if (IsEmpty()) return true;
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));

  bool res = true;
  if (node->IsLeafPage())
  {
    auto pg = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    int size = pg->GetSize();
      res = res && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    for (int i = 1; i < size; i++)
    {
      if (comparator_(pg->KeyAt(i - 1), pg->KeyAt(i)) > 0)
      {
          res = false;
        break;
      }
    }
    out = pair<KeyType,KeyType>{pg->KeyAt(0), pg->KeyAt(size - 1)};
  }
  else {
    auto pg = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    int siz = pg->GetSize();
      res = res && (siz >= node->GetMinSize() && siz <= node->GetMaxSize());
    pair<KeyType,KeyType> left,right;
    for (int i = 1; i < siz; i++)
    {
      if (i == 1) {
          res = res && isPageCorr(pg->ValueAt(0), left);
      }
        res = res && isPageCorr(pg->ValueAt(i), right);
        res = res && (comparator_(pg->KeyAt(i) , left.second) > 0 && comparator_(pg->KeyAt(i), right.first) <= 0);
        res = res && (i == 1 || comparator_(pg->KeyAt(i - 1) , pg->KeyAt(i)) < 0);
      if (!res) break;
      left = right;
    }
    out = pair<KeyType,KeyType>{pg->KeyAt(0), pg->KeyAt(siz - 1)};
  }
  buffer_pool_manager_->UnpinPage(pid,false);
  return res;
}

//全面检查树是否正确
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check(bool forceCheck) {
  if (!forceCheck && !openCheck) return true;
  pair<KeyType,KeyType> in;
  bool isBalance= (isBalanced(root_page_id_) >= 0);
  bool isPageInOrderAndSizeCorr = isPageCorr(root_page_id_, in);
  bool isAllUnpin = buffer_pool_manager_->CheckAllUnpined();

  if (!isBalance) cout << "problems in balance" << endl;
  if (!isPageInOrderAndSizeCorr) cout<<"problems in page order or page size"<<endl;
  if (!isAllUnpin) cout<<"problems in page unpin"<<endl;

  return isPageInOrderAndSizeCorr && isBalance && isAllUnpin;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
} // namespace scudb
