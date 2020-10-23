//
// Created by Ankush J on 10/22/20.
//

#pragma once

#include <stdlib.h>

#include <iterator>
#include <string>

#define MB(x) (static_cast<size_t>(x) << 20u)

namespace pdlfs {
struct SortableItem {
  float key;
  char other_bytes[44];
};

struct SortableItemComparator {
  bool operator() (const SortableItem& a, const SortableItem& b) {
    return a.key < b.key;
  }
};

class SortableBuffer {
 public:
  SortableBuffer();
  ~SortableBuffer();
  int AddFile(std::string& file_path);
  void Sort();
  void Clear();
  int FindBounds(float range_start, float range_end);
  const float& operator[](size_t idx);

 private:
  size_t item_size_;

  SortableItem* buf_;
  size_t mem_allocated_;
  size_t mem_written_;
  static constexpr size_t kAllocDefault = MB(10);

  void Reallocate();
  static int FileExists(std::string& file_path, size_t& file_size);
};
}  // namespace pdlfs