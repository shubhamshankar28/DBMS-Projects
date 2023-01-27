//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}


void LRUKReplacer::DeleteFrame(frame_id_t frame_id) {
    if(accessHistory.find(frame_id) != accessHistory.end())
        accessHistory.erase(frame_id);
    if(evictablePages.find(frame_id) != evictablePages.end())
        evictablePages.erase(frame_id);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    

    if(curr_size_ == 0) {
        return false;
    }

    frame_id_t ans = -1;
    long long int val = -1;
    long long int timeS = inf+5;

    for(auto it : accessHistory) {
        if(evictablePages[it.first]) {
            long long int temp , tempS;
            if(accessHistory[it.first].size() < k_) {
                temp = inf;
                if(accessHistory[it.first].size() > 0) {
                    tempS = accessHistory[it.first].back();
                }
                else {
                    tempS = -1;
                }
            }
            else {
                temp = current_timestamp_ - accessHistory[it.first].front();
                tempS = accessHistory[it.first].back();
            }
            if(val < temp) {
                val = temp;
                ans = it.first;
                timeS = tempS;
            }
            else if(temp == val) {
                assert(val == inf);
                if(tempS < timeS) {
                    timeS = tempS;
                    ans = it.first;
                    val = temp;
                }
            }
        }   
    }

    curr_size_--;
    DeleteFrame(ans);
    *frame_id = ans;
    return true; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT(((size_t)frame_id < replacer_size_), "Invalid Frame ID");
    current_timestamp_++;
    if((accessHistory.find(frame_id) != accessHistory.end()) && (accessHistory[frame_id].size() == k_)) {
        accessHistory[frame_id].pop();
    }
    accessHistory[frame_id].push(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(((size_t)frame_id < replacer_size_), "Invalid Frame ID");

    if(evictablePages.find(frame_id) == evictablePages.end()) {
        if(set_evictable) {
            curr_size_++;
        }
    }
    else {
        if(evictablePages[frame_id] != set_evictable) {
            if(set_evictable) {
                curr_size_++;
            }
            else {
                curr_size_--;
            }
        }
    }

    evictablePages[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    BUSTUB_ASSERT(((size_t)frame_id < replacer_size_), "Invalid Frame ID");
    if(evictablePages.find(frame_id) == evictablePages.end())
        return;
    BUSTUB_ASSERT(evictablePages[frame_id], "Invalid Frame ID");
    curr_size_--;
    DeleteFrame(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
