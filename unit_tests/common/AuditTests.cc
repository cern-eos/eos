//------------------------------------------------------------------------------
// File: AuditTests.cc
// Author: EOS Team - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/Audit.hh"
#include "proto/Audit.pb.h"

#include "Namespace.hh"
#include "gtest/gtest.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include <string>
#include <set>
#include <chrono>
#include <thread>
#include <vector>

EOSCOMMONTESTING_BEGIN

namespace {
std::string makeTempDir()
{
  std::string base = "/tmp/eos_audit_test_XXXXXX";
  std::vector<char> buf(base.begin(), base.end());
  buf.push_back('\0');
  char* r = ::mkdtemp(buf.data());
  if (!r) {
    throw std::runtime_error("mkdtemp failed");
  }
  return std::string(r);
}

std::set<std::string> listZst(const std::string& dir)
{
  std::set<std::string> out;
  DIR* d = ::opendir(dir.c_str());
  if (!d) return out;
  struct dirent* ent;
  while ((ent = ::readdir(d)) != nullptr) {
    if (ent->d_name[0] == '.') continue;
    std::string name = ent->d_name;
    if (name.size() >= 4 && name.substr(name.size() - 4) == ".zst") {
      out.insert(name);
    }
  }
  ::closedir(d);
  return out;
}

std::string readSymlink(const std::string& path)
{
  char buf[PATH_MAX];
  ssize_t n = ::readlink(path.c_str(), buf, sizeof(buf) - 1);
  if (n < 0) return "";
  buf[n] = '\0';
  return std::string(buf);
}
}

TEST(Audit, BasicWriteRotateAndSymlink)
{
  using namespace eos::common;
  std::string dir = makeTempDir();

  Audit audit(dir, /*rotationSeconds*/1, /*compressionLevel*/1);

  eos::audit::AuditRecord rec1;
  rec1.set_timestamp(::time(nullptr));
  rec1.set_path("/eos/test/file1");
  rec1.set_operation(eos::audit::Operation::CREATE);
  rec1.set_client_ip("127.0.0.1");
  rec1.set_account("root");
  rec1.mutable_auth()->set_mechanism("local");
  rec1.mutable_authorization()->add_reasons("uidgid");

  audit.audit(rec1);

  auto files1 = listZst(dir);
  ASSERT_GE(files1.size(), 1u);

  std::string linkPath = dir + "/audit.zstd";
  struct stat st{};
  ASSERT_EQ(::lstat(linkPath.c_str(), &st), 0);
  ASSERT_TRUE(S_ISLNK(st.st_mode));
  std::string firstTarget = readSymlink(linkPath);
  ASSERT_FALSE(firstTarget.empty());

  std::this_thread::sleep_for(std::chrono::seconds(2));

  eos::audit::AuditRecord rec2;
  rec2.set_timestamp(::time(nullptr));
  rec2.set_path("/eos/test/file2");
  rec2.set_operation(eos::audit::Operation::RENAME);
  rec2.set_target("/eos/test/file2.new");
  rec2.set_client_ip("127.0.0.1");
  rec2.set_account("root");
  rec2.mutable_auth()->set_mechanism("local");
  rec2.mutable_authorization()->add_reasons("uidgid");

  audit.audit(rec2);

  auto files2 = listZst(dir);
  ASSERT_GE(files2.size(), 2u);

  std::string secondTarget = readSymlink(linkPath);
  ASSERT_FALSE(secondTarget.empty());
  ASSERT_NE(firstTarget, secondTarget);
}

TEST(Audit, BenchmarkWrite10000)
{
  using namespace eos::common;
  std::string dir = makeTempDir();

  // Use a long rotation to avoid rotation overhead in benchmark
  Audit audit(dir, /*rotationSeconds*/3600, /*compressionLevel*/1);

  const int N = 100000;
  eos::audit::AuditRecord rec;
  rec.set_timestamp(::time(nullptr));
  rec.set_path("/eos/bench/file");
  rec.set_operation(eos::audit::WRITE);
  rec.set_client_ip("127.0.0.1");
  rec.set_account("bench");
  rec.set_svc("test");

  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < N; ++i) {
    rec.set_path(std::string("/eos/bench/file_") + std::to_string(i));
    audit.audit(rec);
  }
  auto t1 = std::chrono::steady_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  std::cout << "Audit benchmark: wrote " << N << " records in " << ms << " ms\n";
}

TEST(Audit, BenchmarkWrite100kConcurrent)
{
  using namespace eos::common;
  std::string dir = makeTempDir();

  // Long rotation to avoid rotation overhead skewing results
  Audit audit(dir, /*rotationSeconds*/3600, /*compressionLevel*/1);

  const int numThreads = 100;
  const int perThread = 1000; // 100 * 1000 = 100000

  eos::audit::AuditRecord base;
  base.set_timestamp(::time(nullptr));
  base.set_operation(eos::audit::WRITE);
  base.set_client_ip("127.0.0.1");
  base.set_account("bench");
  base.set_svc("test");

  std::vector<std::thread> threads;
  threads.reserve(numThreads);

  auto t0 = std::chrono::steady_clock::now();
  for (int t = 0; t < numThreads; ++t) {
    threads.emplace_back([&, t]() {
      eos::audit::AuditRecord rec = base;
      for (int i = 0; i < perThread; ++i) {
        rec.set_path(std::string("/eos/bench/concurrent_") + std::to_string(t) + "_" + std::to_string(i));
        audit.audit(rec);
      }
    });
  }
  for (auto& th : threads) th.join();
  auto t1 = std::chrono::steady_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  std::cout << "Audit concurrent benchmark: wrote " << (numThreads * perThread)
            << " records in " << ms << " ms\n";
}

EOSCOMMONTESTING_END


