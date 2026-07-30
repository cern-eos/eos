//------------------------------------------------------------------------------
// File: BlockXsMapTests.cc
// Author: Elvin Sindrilaru <esindril@cern.ch>
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

#include "fst/checksum/Adler.hh"
#include "gtest/gtest.h"
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace eos::fst;

//------------------------------------------------------------------------------
//! Tests for the mmapped block checksum map and in particular for the recovery
//! from a SIGBUS raised by an access to it. In production such a fault is
//! raised when the filesystem holding the map file runs out of space: ChangeMap
//! extends the map file sparsely with ftruncate and the first store into a
//! freshly mapped page can then not allocate a block anymore. The tests below
//! reproduce the very same fault by shrinking the map file while it stays
//! mapped, which makes any access beyond the new end of file raise SIGBUS.
//------------------------------------------------------------------------------
class BlockXsMapTest : public ::testing::Test {
protected:
  static constexpr size_t sBlockSize = 4096;
  static constexpr size_t sNumBlocks = 64;

  void
  SetUp() override
  {
    char tmpl[] = "/tmp/eos_blockxs_test.XXXXXX";
    const char* dir = mkdtemp(tmpl);
    ASSERT_NE(dir, nullptr);
    mDir = dir;
    mPath = mDir + "/data";
    mPathXs = mDir + "/data.xsmap";
    // Populate the data file with deterministic, non-repeating content
    std::string buffer(sBlockSize, '\0');
    int fd = open(mPath.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    for (size_t i = 0; i < sNumBlocks; ++i) {
      memset(&buffer[0], (int)('a' + (i % 26)), sBlockSize);
      ASSERT_EQ(write(fd, buffer.data(), sBlockSize), (ssize_t)sBlockSize);
    }

    close(fd);
    mFd = open(mPath.c_str(), O_RDONLY);
    ASSERT_GE(mFd, 0);
  }

  void
  TearDown() override
  {
    if (mFd >= 0) {
      close(mFd);
    }

    std::string cmd = "rm -rf " + mDir;
    (void)system(cmd.c_str());
  }

  //----------------------------------------------------------------------------
  //! Checksum every block but the first one, so that block 0 is the only hole
  //! left for AddBlockSumHoles to fill
  //----------------------------------------------------------------------------
  void
  FillAllButFirstBlock(CheckSum& xs)
  {
    std::string buffer(sBlockSize, '\0');

    for (size_t i = 1; i < sNumBlocks; ++i) {
      ASSERT_EQ(pread(mFd, &buffer[0], sBlockSize, i * sBlockSize), (ssize_t)sBlockSize);
      xs.Reset();
      ASSERT_TRUE(xs.AddBlockSum(i * sBlockSize, buffer.data(), sBlockSize));
    }
  }

  //----------------------------------------------------------------------------
  //! Shrink the map file to zero while it stays mapped by the checksum object
  //----------------------------------------------------------------------------
  void
  ShrinkXsMap()
  {
    int fdxs = open(mPathXs.c_str(), O_RDWR);
    ASSERT_GE(fdxs, 0);
    ASSERT_EQ(ftruncate(fdxs, 0), 0);
    close(fdxs);
  }

  std::string mDir;
  std::string mPath;
  std::string mPathXs;
  int mFd{-1};
};

//------------------------------------------------------------------------------
//! Adler checksum object which shrinks the block checksum map right after
//! AddBlockSum returned. AddBlockSumHoles calls AddBlockSum from inside its own
//! loop over the map, therefore this drops the map from underneath the loop at
//! the point where AddBlockSum - and with it the SetXSMap frame guarding the map
//! writes - has just returned. The next iteration of the loop then faults on a
//! map read whose only valid recovery point is the loop itself.
//------------------------------------------------------------------------------
class ShrinkingAdler : public Adler {
public:
  ShrinkingAdler(const std::string& pathxs)
      : mPathXs(pathxs)
  {
  }

  void
  EnableShrink()
  {
    mShrink = true;
  }

  bool
  AddBlockSum(off_t offset, const char* buffer, size_t len) override
  {
    bool rc = Adler::AddBlockSum(offset, buffer, len);

    if (mShrink) {
      mShrink = false;
      int fdxs = open(mPathXs.c_str(), O_RDWR);

      if (fdxs >= 0) {
        (void)!ftruncate(fdxs, 0);
        close(fdxs);
      }
    }

    return rc;
  }

private:
  std::string mPathXs;
  bool mShrink{false};
};

//------------------------------------------------------------------------------
// Baseline: fill the holes and verify the whole map
//------------------------------------------------------------------------------
TEST_F(BlockXsMapTest, AddBlockSumHolesFillsAndVerifies)
{
  Adler xs;
  ASSERT_TRUE(xs.OpenMap(mPathXs.c_str(), sNumBlocks * sBlockSize, sBlockSize, true));
  FillAllButFirstBlock(xs);
  ASSERT_TRUE(xs.ChangeMap(sNumBlocks * sBlockSize, true));
  ASSERT_TRUE(xs.AddBlockSumHoles(mFd));
  ASSERT_GE(xs.GetXSBlocksWrittenHoles(), 1ull);
  // Every block must now carry a checksum matching the data
  std::string buffer(sBlockSize, '\0');

  for (size_t i = 0; i < sNumBlocks; ++i) {
    ASSERT_EQ(pread(mFd, &buffer[0], sBlockSize, i * sBlockSize), (ssize_t)sBlockSize);
    xs.Reset();
    ASSERT_TRUE(xs.Add(buffer.data(), sBlockSize, 0));
    xs.Finalize();
    ASSERT_TRUE(xs.VerifyXSMap(i * sBlockSize)) << "block " << i;
  }

  ASSERT_TRUE(xs.CloseMap());
}

//------------------------------------------------------------------------------
// A SIGBUS on the very first guarded map read of AddBlockSumHoles must be
// reported as a failure instead of killing the process
//------------------------------------------------------------------------------
TEST_F(BlockXsMapTest, AddBlockSumHolesRecoversFromSigBus)
{
  Adler xs;
  ASSERT_TRUE(xs.OpenMap(mPathXs.c_str(), sNumBlocks * sBlockSize, sBlockSize, true));
  FillAllButFirstBlock(xs);
  ASSERT_TRUE(xs.ChangeMap(sNumBlocks * sBlockSize, true));
  ShrinkXsMap();
  ASSERT_FALSE(xs.AddBlockSumHoles(mFd));
  ASSERT_TRUE(xs.CloseMap());
}

//------------------------------------------------------------------------------
// Regression test for the crash reported on FST close: a SIGBUS raised in the
// AddBlockSumHoles map read after AddBlockSum -> SetXSMap has returned used to
// siglongjmp into the retired SetXSMap frame, because both armed the same
// process wide jump buffer. That resurrected frame then faulted with a null
// "this" inside its own recovery printout, taking the FST down with a SIGSEGV.
//------------------------------------------------------------------------------
TEST_F(BlockXsMapTest, AddBlockSumHolesRecoversFromSigBusAfterNestedGuard)
{
  ShrinkingAdler xs(mPathXs);
  ASSERT_TRUE(xs.OpenMap(mPathXs.c_str(), sNumBlocks * sBlockSize, sBlockSize, true));
  FillAllButFirstBlock(xs);
  ASSERT_TRUE(xs.ChangeMap(sNumBlocks * sBlockSize, true));
  // From here on the first AddBlockSum call drops the map. AddBlockSumHoles
  // fills the hole at block 0 and then faults reading the map for block 1.
  xs.EnableShrink();
  ASSERT_FALSE(xs.AddBlockSumHoles(mFd));
  ASSERT_TRUE(xs.CloseMap());
}

//------------------------------------------------------------------------------
// The same for the read side used by the scanner
//------------------------------------------------------------------------------
TEST_F(BlockXsMapTest, VerifyXSMapRecoversFromSigBus)
{
  Adler xs;
  ASSERT_TRUE(xs.OpenMap(mPathXs.c_str(), sNumBlocks * sBlockSize, sBlockSize, true));
  FillAllButFirstBlock(xs);
  ASSERT_TRUE(xs.ChangeMap(sNumBlocks * sBlockSize, true));
  ShrinkXsMap();
  std::string buffer(sBlockSize, '\0');
  ASSERT_EQ(pread(mFd, &buffer[0], sBlockSize, sBlockSize), (ssize_t)sBlockSize);
  xs.Reset();
  ASSERT_TRUE(xs.Add(buffer.data(), sBlockSize, 0));
  xs.Finalize();
  ASSERT_FALSE(xs.VerifyXSMap(sBlockSize));
  ASSERT_TRUE(xs.CloseMap());
}

//------------------------------------------------------------------------------
// Recovering from a SIGBUS must not leave the jump buffer armed, otherwise a
// later unrelated SIGBUS would be swallowed. Fault repeatedly and make sure
// every attempt is reported as a failure and the object stays usable.
//------------------------------------------------------------------------------
TEST_F(BlockXsMapTest, RepeatedSigBusIsRecoveredEveryTime)
{
  Adler xs;
  ASSERT_TRUE(xs.OpenMap(mPathXs.c_str(), sNumBlocks * sBlockSize, sBlockSize, true));
  FillAllButFirstBlock(xs);
  ASSERT_TRUE(xs.ChangeMap(sNumBlocks * sBlockSize, true));
  ShrinkXsMap();

  for (int i = 0; i < 5; ++i) {
    ASSERT_FALSE(xs.AddBlockSumHoles(mFd)) << "attempt " << i;
  }

  ASSERT_TRUE(xs.CloseMap());
}

//------------------------------------------------------------------------------
// Two threads faulting concurrently must each recover on their own stack. With
// a process wide jump buffer indexed by the thread id one thread could jump
// onto the stack of the other one.
//------------------------------------------------------------------------------
TEST_F(BlockXsMapTest, ConcurrentSigBusRecovery)
{
  constexpr int num_threads = 8;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, &failures]() {
      const std::string path = mDir + "/data" + std::to_string(t);
      const std::string pathxs = path + ".xsmap";
      std::string buffer(sBlockSize, 'x');
      int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);

      if (fd < 0) {
        ++failures;
        return;
      }

      for (size_t i = 0; i < sNumBlocks; ++i) {
        if (write(fd, buffer.data(), sBlockSize) != (ssize_t)sBlockSize) {
          ++failures;
          close(fd);
          return;
        }
      }

      Adler xs;

      if (!xs.OpenMap(pathxs.c_str(), sNumBlocks * sBlockSize, sBlockSize, true)) {
        ++failures;
        close(fd);
        return;
      }

      for (size_t i = 1; i < sNumBlocks; ++i) {
        xs.Reset();

        if (!xs.AddBlockSum(i * sBlockSize, buffer.data(), sBlockSize)) {
          ++failures;
        }
      }

      // Drop this thread's own map and fault on it
      int fdxs = open(pathxs.c_str(), O_RDWR);

      if (fdxs >= 0) {
        (void)!ftruncate(fdxs, 0);
        close(fdxs);
      }

      if (xs.AddBlockSumHoles(fd)) {
        ++failures;
      }

      xs.CloseMap();
      close(fd);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_EQ(failures.load(), 0);
}

//------------------------------------------------------------------------------
// A SIGBUS raised outside any guarded map access must reach the handler which
// was installed before OpenMap installed its own, so that the FST stacktrace
// handler keeps working. Checked in a child process since the fault is fatal.
//------------------------------------------------------------------------------
TEST_F(BlockXsMapTest, UnguardedSigBusReachesPreviousHandler)
{
  pid_t pid = fork();
  ASSERT_GE(pid, 0);

  if (pid == 0) {
    // Stand in for the FST stacktrace handler installed at startup
    if (signal(SIGBUS, [](int) { _exit(42); }) == SIG_ERR) {
      _exit(1);
    }

    Adler xs;

    // This installs the CheckSum SIGBUS handler over the one above
    if (!xs.OpenMap(mPathXs.c_str(), sNumBlocks * sBlockSize, sBlockSize, true)) {
      _exit(2);
    }

    // Fault on a mapping which has nothing to do with the block checksum map
    const std::string other = mDir + "/unrelated";
    int ofd = open(other.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);

    if ((ofd < 0) || ftruncate(ofd, sBlockSize)) {
      _exit(3);
    }

    char* addr = (char*)mmap(0, sBlockSize, PROT_READ | PROT_WRITE, MAP_SHARED, ofd, 0);

    if ((addr == MAP_FAILED) || ftruncate(ofd, 0)) {
      _exit(4);
    }

    addr[0] = 'x'; // SIGBUS
    _exit(5);      // no fault was raised
  }

  int status = 0;
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status))
      << "child died on signal " << (WIFSIGNALED(status) ? WTERMSIG(status) : 0);
  ASSERT_EQ(WEXITSTATUS(status), 42);
}
