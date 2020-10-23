//
// Created by Ankush J on 10/15/20.
//

#include "query_client.h"

#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>

void clear() {
  int fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
  write(fd, "1", 1);
  close(fd);
}

uint64_t now_us() {
  struct timespec tv;
  clock_gettime(CLOCK_MONOTONIC, &tv);
  uint64_t t;
  t = static_cast<uint64_t>(tv.tv_sec) * 1000000;
  t += tv.tv_nsec / 1000;
  return t;
}

namespace pdlfs {
QueryClient::QueryClient(std::string& manifest_path, std::string& data_path)
    : manifest_path_(manifest_path), data_path_(data_path) {}

int QueryClient::LoadManifest() {
  int rank = 0;
  int rv = 0;

  while (true) {
    std::string man_path =
        manifest_path_ + "/vpic-manifest." + std::to_string(rank);

    int rv = manifest_.PopulateFromDisk(man_path, rank);
    if (rv < 0) break;

    rank++;
  }

  manifest_loaded_ = true;
  return rv;
}

void QueryClient::RangeQuery(float start, float end) {
  PartitionManifestMatch match_obj;
  if (!manifest_loaded_) LoadManifest();

  manifest_.GetOverLappingEntries(start, end, match_obj);

  printf("Matched: %zu\n", match_obj.items.size());

  buf_.Clear();

  for (size_t i = 0; i < match_obj.items.size(); i++) {
    PartitionManifestItem& item = match_obj.items[i];
    std::string sst_path = data_path_ + "/bucket." +
                           std::to_string(item.rank) + "." +
                           std::to_string(item.bucket_idx);

    buf_.AddFile(sst_path);
  }

  buf_.Sort();
  buf_.FindBounds(start, end);
}
};  // namespace pdlfs