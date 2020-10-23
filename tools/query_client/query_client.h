//
// Created by Ankush J on 10/15/20.
//

#pragma once

#include <string>

#include "range_backend/range_backend.h"
#include "sortable_buffer.h"

namespace pdlfs {
class QueryClient {
 public:
  QueryClient(std::string& manifest_path, std::string& data_path);
  void RangeQuery(float start, float end);

 private:
  int LoadManifest();

  std::string manifest_path_;
  std::string data_path_;

  SortableBuffer buf_;
  PartitionManifest manifest_;

  bool manifest_loaded_ = false;
};
};  // namespace pdlfs