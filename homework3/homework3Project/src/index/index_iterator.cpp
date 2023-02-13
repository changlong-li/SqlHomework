/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace scudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bufferPoolManager): idx_(index),the_leaf_(leaf), buffer_pool_manager_(bufferPoolManager){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    if (the_leaf_ != nullptr) {
        UnlockAndUnPin();
    }
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::UnlockAndUnPin()
{
    buffer_pool_manager_->FetchPage(the_leaf_->GetPageId())->RUnlatch();
    buffer_pool_manager_->UnpinPage(the_leaf_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(the_leaf_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool IndexIterator<KeyType, ValueType, KeyComparator>::isEnd()
{
    if(the_leaf_ == nullptr)
        return true;
    else if(idx_ == the_leaf_->GetSize() && the_leaf_->GetNextPageId() == INVALID_PAGE_ID)
        return true;
    else
        return false;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &IndexIterator<KeyType, ValueType, KeyComparator>::operator*()
{
    return the_leaf_->GetItem(idx_);
}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator<KeyType, ValueType, KeyComparator> &IndexIterator<KeyType, ValueType, KeyComparator>::operator++()
{
    idx_++;
    if(idx_ == the_leaf_->GetSize() && the_leaf_->GetNextPageId() != INVALID_PAGE_ID)
    {
        page_id_t  nextPageId = the_leaf_->GetNextPageId();

        auto *pg = buffer_pool_manager_->FetchPage(nextPageId);

        pg->RLatch();
        UnlockAndUnPin();

        auto nextLeaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(pg->GetData());

        idx_ = 0;
        the_leaf_ = nextLeaf;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
