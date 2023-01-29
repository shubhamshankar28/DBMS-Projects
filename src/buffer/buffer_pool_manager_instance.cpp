//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  // page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  // delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::RetrieveFreePage(frame_id_t * id) -> int {
  if(!free_list_.empty()) {
    *id = free_list_.front();
    free_list_.pop_front();
    return 0;
  }

  if(replacer_->Evict(id)) {
    if(pages_[*id].is_dirty_) {
      disk_manager_->WritePage(pages_[*id].page_id_,pages_[*id].data_);
    }

    if(page_to_frame_.find(pages_[*id].page_id_) != page_to_frame_.end()) {
      page_to_frame_.erase(page_to_frame_.find(pages_[*id].page_id_));
    }
    return 1;
  }

  return 2;
}


void BufferPoolManagerInstance::ResetPageInfo(frame_id_t  id) {
  pages_[id].is_dirty_ = false;
  pages_[id].pin_count_ = 0;
  pages_[id].page_id_ = -1;
  pages_[id].ResetMemory();
} 

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
  frame_id_t frame_id;
  
  int status = RetrieveFreePage(&frame_id);
  if(status == 2) {
    return nullptr;
  }

  ResetPageInfo(frame_id);

  pages_[frame_id].pin_count_++;
  replacer_->SetEvictable(frame_id,false);
  replacer_->RecordAccess(frame_id);
  
  *page_id = pages_[frame_id].page_id_ = AllocatePage();

  page_to_frame_[*page_id] = frame_id;

  // std::cout<<"putting "<<(*page_id)<<" in "<<(*frame_id)<<"\n";

  return (pages_ + frame_id);
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  
  frame_id_t  frame_id;
  if(page_to_frame_.find(page_id) != page_to_frame_.end()) {
    
    frame_id = page_to_frame_[page_id];

    // std::cout<<"Fetch found "<<page_id<<" in "<<(*frame_id)<<"\n";
    pages_[frame_id].pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    
    return (pages_ + frame_id);
  }
  else {
    int status = RetrieveFreePage(&frame_id);
    if(status == 2) {
      return nullptr;
    }


    // std::cout<<"Fetch had to load "<<page_id<<" in "<<(*frame_id)<<"\n";
    ResetPageInfo(frame_id);

    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_++;
    disk_manager_->ReadPage(page_id,pages_[frame_id].data_);
    replacer_->SetEvictable(frame_id,false);
    replacer_->RecordAccess(frame_id);

    page_to_frame_[page_id] = frame_id;
    
    return (pages_ + frame_id);
  }

  return nullptr; 
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // std::cout<<"unpin called on "<<page_id<<"\n"; 
  frame_id_t  frame_id;
  if(page_to_frame_.find(page_id) != page_to_frame_.end()) {
    frame_id = page_to_frame_[page_id];
    int value;
    if((value = pages_[frame_id].pin_count_) == 0) {
      // std::cout<<"unpin returns false because pin_count is already 0\n";
      return false;
    }
    value--;
    pages_[frame_id].pin_count_ = value;
    if(value == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    pages_[frame_id].is_dirty_ = is_dirty;
    return true;
  }
  else {
    // std::cout<<"unpin returns false because "<<page_id<<" not found\n";
    return false;
  } 
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  frame_id_t frame_id;
  if(page_to_frame_.find(page_id) != page_to_frame_.end()) {
    frame_id = page_to_frame_[page_id];
    disk_manager_->WritePage(page_id,pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
    return true;
  }

  return false; 
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for(auto it : page_to_frame_) {
    FlushPgImp(it.first);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  frame_id_t frame_id;
  if(page_to_frame_.find(page_id) != page_to_frame_.end()) {
    frame_id = page_to_frame_[page_id];
    if((pages_[frame_id].is_dirty_) || (pages_[frame_id].pin_count_ != 0)) {
      return false;
    }

    ResetPageInfo(frame_id);
    free_list_.push_back(frame_id);
    page_to_frame_.erase(page_to_frame_.find(page_id));
    DeallocatePage(page_id);
    
    return true;
  }

  return true; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
