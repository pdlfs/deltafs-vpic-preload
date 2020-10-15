//
// Created by Ankush J on 10/15/20.
//

#include <string>

#include "query_client/query_client.h"

int main() {
  std::string p1, p2;
  QueryClient client(p1, p2);
  client.Run();
  return 0;
}
