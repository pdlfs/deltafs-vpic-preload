//
// Created by Ankush J on 4/2/21.
//

#include "carp/stat_trigger.h"

#include <pdlfs-common/env.h>

#include <iostream>

#include "carp/carp.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {
namespace carp {
class StatTriggerTest {
 public:
  StatTriggerTest() { env_ = Env::Default(); }
  void CreateFile(const std::string& fpath, size_t fsize) {
    Status s = Status::OK();

    std::string data_str;
    data_str.resize(fsize);
    Slice data = data_str;

    WritableFile* fh;
    s = env_->NewWritableFile(fpath.c_str(), &fh);
    ASSERT_TRUE(s.ok());
    s = fh->Append(data);
    ASSERT_TRUE(s.ok());
    s = fh->Close();
    ASSERT_TRUE(s.ok());

    delete fh;
  }

  std::string MockDirPath() {
    Status s = Status::OK();
    std::string tmp_dir;

    s = env_->GetTestDirectory(&tmp_dir);
    ASSERT_TRUE(s.ok());

    std::string plfs_dir = tmp_dir + "/particle";

    return plfs_dir;
  }

  void CreateMockDir(int nranks) {
    Status s = Status::OK();

    std::string plfs_dir = MockDirPath();
    std::cout << plfs_dir << "\n";

    DeleteMockDir();

    s = env_->CreateDir(plfs_dir.c_str());
    ASSERT_TRUE(s.ok());

    std::vector<std::string> rank_paths =
        StatTriggerUtils::RankPaths(plfs_dir, nranks);

    for (int rank = 0; rank < nranks; rank++) {
      const std::string& rank_fname = rank_paths[rank];
      CreateFile(rank_fname, 4096);
    }
  }

  void DeleteMockDir() {
    Status s = Status::OK();

    std::string plfs_dir = MockDirPath();
    std::cout << plfs_dir << "\n";

    if (env_->FileExists(plfs_dir.c_str())) {
      std::vector<std::string> rdb_files;

      s = env_->GetChildren(plfs_dir.c_str(), &rdb_files);
      ASSERT_TRUE(s.ok());

      for (size_t i = 0; i < rdb_files.size(); i++) {
        std::string f = rdb_files[i];
        if (f == "." || f == "..") continue;
        f = plfs_dir + "/" + f;
        s = env_->DeleteFile(f.c_str());
        ASSERT_TRUE(s.ok());
      }

      s = env_->DeleteDir(plfs_dir.c_str());
      ASSERT_TRUE(s.ok());
    }
  }

  Env* env_;
};

TEST(StatTriggerTest, FileNames) {
  ASSERT_EQ(StatTriggerUtils::RankToFname(0), "RDB-00000000.tbl");

  for (int rank = 0; rank < 5000; rank++) {
    std::string fname = StatTriggerUtils::RankToFname(rank);
    ASSERT_EQ(StatTriggerUtils::FnameToRank(fname), rank);
  }
}

TEST(StatTriggerTest, DirUtils) {
  CreateMockDir(4);
  DeleteMockDir();
}

TEST(StatTriggerTest, BasicFunction) {
  int nranks = 4;
  CreateMockDir(nranks);
  std::string dir_path = MockDirPath();

  CarpOptions options;
  options.my_rank = 0;
  options.num_ranks = nranks;
  options.mount_path = dir_path;
  options.dynamic_intvl = 1;
  options.dynamic_thresh = 1.2;
  options.env = Env::Default();

  StatTrigger st(options);
  bool trig = st.Invoke();
  ASSERT_FALSE(trig);
}
}  // namespace carp
}  // namespace pdlfs
int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
