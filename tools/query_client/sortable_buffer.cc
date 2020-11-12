//
// Created by Ankush J on 10/22/20.
//

#include "sortable_buffer.h"

#include <assert.h>
#include <sys/stat.h>

#include <algorithm>

namespace pdlfs {
SortableBuffer::SortableBuffer()
    : item_size_(sizeof(SortableItem)),
      buf_(nullptr),
      mem_allocated_(0u),
      mem_written_(0u) {}

int SortableBuffer::AddFile(std::string& file_path) {
  size_t file_size;
  int rv = FileExists(file_path, file_size);
  if (rv) return rv;

  while (mem_allocated_ - mem_written_ < file_size) Reallocate();

  FILE* f = fopen(file_path.c_str(), "rb");
  if (f == nullptr) return -1;
  size_t cur_written =
      fread(&reinterpret_cast<char*>(buf_)[mem_written_], 1, file_size, f);
  fclose(f);

  assert(cur_written == file_size);
  assert(file_size % item_size_ == 0u);

  mem_written_ += cur_written;

  return 0;
}

void SortableBuffer::Sort() {
  size_t items = mem_written_ / sizeof(SortableItem);
  std::sort(buf_, buf_ + items, SortableItemComparator());
}

void SortableBuffer::Clear() { mem_written_ = 0; }

int SortableBuffer::FindBounds(float range_start, float range_end) {
  size_t buf_items = mem_written_ / item_size_;
  SortableItem start_item{range_start};
  SortableItem end_item{range_end};

  SortableItem* ptr_start = std::lower_bound(buf_, buf_ + buf_items, start_item,
                                             SortableItemComparator());
  SortableItem* ptr_end = std::upper_bound(ptr_start, buf_ + buf_items,
                                           end_item, SortableItemComparator());

  size_t index_begin = ptr_start - buf_;
  size_t index_end = ptr_end - buf_;

  printf("Found %zu items between %zu and %zu\n", index_end - index_begin,
         index_begin, index_end);

  return 0;
}

const float& SortableBuffer::operator[](size_t idx) { return buf_[idx].key; }

void SortableBuffer::Reallocate() {
  size_t alloc_size = buf_ == nullptr ? kAllocDefault : mem_allocated_ * 2;

  void* new_ptr = realloc(buf_, alloc_size);
  if (new_ptr) {
    mem_allocated_ = alloc_size;
    buf_ = reinterpret_cast<SortableItem*>(new_ptr);
  } else {
    throw std::runtime_error("Reallocate failed");
  }
}

int SortableBuffer::FileExists(std::string& file_path, size_t& file_size) {
  struct stat st;
  int rv = stat(file_path.c_str(), &st);

  if (rv) return rv;

  file_size = st.st_size;
  return 0;
}

SortableBuffer::~SortableBuffer() {
  if (buf_) free(buf_);
}
}  // namespace pdlfs
