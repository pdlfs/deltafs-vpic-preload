//
// Created by Ankush J on 10/15/20.
//

#include <string>

#include "query_client/query_client.h"

int main() {
  std::string base_path = "/Users/schwifty/Repos/workloads/rundata/query-data/data";
  std::string manifest_path = base_path + "/manifests";
  std::string data_path = base_path + "/buckets";

  pdlfs::QueryClient client(manifest_path, data_path);
  client.RangeQuery(0.0001, 0.0004);
  return 0;
}
