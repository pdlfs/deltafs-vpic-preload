//
// Created by Ankush J on 10/15/20.
//

#pragma once

#include <string>

typedef struct {
  float f;
  int data[36];
} data_t;

class QueryClient {
 public:
  QueryClient(std::string& manifest_path, std::string& data_path);
  ~QueryClient();
  void Run();
  void ReadAllReg(int num_ssts);
  void ReadAllMmap();
  void ReadFile(std::string& fpath, off_t size);

 private:
  std::string manifest_path_;
  std::string data_path_;
  data_t* data_store;
  char* cur_ptr;
};
