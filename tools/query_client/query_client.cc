//
// Created by Ankush J on 10/15/20.
//

#include "query_client.h"

#include <assert.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>


#include <algorithm>

void clear() {
  int fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
  write(fd, "1", 1);
  close(fd);
}

QueryClient::QueryClient(std::string& manifest_path, std::string& data_path)
    : manifest_path_(manifest_path), data_path_(data_path) {
  data_store = new data_t[2621400];
  cur_ptr = reinterpret_cast<char*>(data_store);
}

bool data_sort(data_t const& a, data_t const& b) { return a.f < b.f; }

uint64_t now_us() {
  struct timespec tv;
  clock_gettime(CLOCK_MONOTONIC, &tv);
  uint64_t t;
  t = static_cast<uint64_t>(tv.tv_sec) * 1000000;
  t += tv.tv_nsec / 1000;
  return t;
}

void QueryClient::Run() {
  int items_sst = 26214;
  assert(sizeof(data_t) == 40u);
  for (int num_ssts = 1; num_ssts < 100; num_ssts++) {
    int items_total = items_sst * num_ssts;
    // clear();
    cur_ptr = reinterpret_cast<char *>(data_store);
    uint64_t time_begin = now_us();
    ReadAllReg(num_ssts);
    uint64_t time_read = now_us();
    std::sort(data_store, data_store + items_total, &data_sort);
    uint64_t time_end = now_us();

    printf("%d,%lu,%lu,%f\n", num_ssts, (time_end - time_begin) / 1000,
           (time_read - time_begin) / 1000, data_store[0].f);
  }
}

void QueryClient::ReadAllMmap() {}
void QueryClient::ReadAllReg(int num_ssts) {
  std::string query_path = "/users/ankushj/runs/query-data";
  // query_path = "/Users/schwifty/Repos/workloads/rundata/query-data";

  struct dirent* de;
  struct stat statbuf;

  DIR* dr = opendir(query_path.c_str());
  int num_read = 0;

  while ((de = readdir(dr)) != NULL) {
    //    printf("%s\n", de->d_name);
    std::string full_path = query_path + "/" + de->d_name;
    //    printf("%s\n", full_path.c_str());
    stat(full_path.c_str(), &statbuf);
    //    printf("%lld\n", statbuf.st_size);
    if (statbuf.st_size < 10000) continue;
    ReadFile(full_path, statbuf.st_size);
    if (num_read++ == num_ssts) break;
  }
  closedir(dr);
  return;
}

QueryClient::~QueryClient() { delete[] data_store; }

void QueryClient::ReadFile(std::string& fpath, off_t size) {
  FILE* f = fopen(fpath.c_str(), "r");
  size_t items_read = fread(cur_ptr, 40, size / 40, f);
  fclose(f);

  //  printf("%u %u\n", (uint32_t)items_read, (uint32_t)size);
  assert(items_read == size / 40);
  cur_ptr += size;
}
