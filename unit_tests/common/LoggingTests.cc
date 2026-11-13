//------------------------------------------------------------------------------
// File: LoggingTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "Namespace.hh"
#include "common/Logging.hh"
#include "gtest/gtest.h"
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

//------------------------------------------------------------------------------
// Test the proper static allocation and destruction of the global logging
// object in Logging.hh
//------------------------------------------------------------------------------

extern void function_using_logging();

EOSCOMMONTESTING_BEGIN

TEST(Logging, Log)
{
  using namespace eos::common;
  gLogging.SetLogPriority(LOG_INFO);
  eos_static_info("%s %s", __FUNCTION__, "print a test log line");
  function_using_logging();
}

//------------------------------------------------------------------------------
// Many producers on a tiny queue: no line is lost or written twice, each
// thread's lines keep their order, a graceful shutdown drains the queue and
// lines logged after the shutdown are dropped.
//------------------------------------------------------------------------------
TEST(Logging, ConcurrentProducers)
{
  using namespace eos::common;
  constexpr int kThreads = 32;
  constexpr int kLines = 2000;
  FILE* fp = tmpfile();
  ASSERT_NE(fp, nullptr);
  LogBuffer* old_lb = gLogging.LB;
  const bool old_stderr = gLogging.gToStderr;
  const int old_level = gLogging.gPriorityLevel;
  const auto old_fanout = gLogging.gLogFanOut;
  LogBuffer lb;
  lb.max_log_buffers = 1; // 8 KB queue: producers must wait for the log thread
  gLogging.LB = &lb;
  gLogging.gToStderr = false;
  gLogging.gLogFanOut.clear();
  gLogging.AddFanOut("*", fp);
  gLogging.SetLogPriority(LOG_INFO);
  std::vector<std::thread> threads;

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([t]() {
      for (int i = 0; i < kLines; ++i) {
        eos_static_info("msg=\"producer test\" thread=%d seq=%d", t, i);
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  lb.shutDown(true);
  const std::string after =
      gLogging.log(__FUNCTION__, __FILE__, __LINE__, "test", gLogging.gZeroVid, "",
                   LOG_INFO, "after shutdown");
  gLogging.LB = old_lb;
  gLogging.gToStderr = old_stderr;
  gLogging.gLogFanOut = old_fanout;
  gLogging.SetLogPriority(old_level);
  EXPECT_TRUE(after.empty());
  EXPECT_EQ(lb.log_buffer_in_q, 0);
  EXPECT_TRUE(lb.pending.empty());
  std::vector<int> next(kThreads, 0);
  int total = 0;
  char line[16384];
  rewind(fp);

  while (fgets(line, sizeof(line), fp)) {
    int t = -1, seq = -1;
    const char* p = strstr(line, "thread=");
    ASSERT_NE(p, nullptr) << line;
    ASSERT_EQ(sscanf(p, "thread=%d seq=%d", &t, &seq), 2) << line;
    ASSERT_TRUE(t >= 0 && t < kThreads) << line;
    ASSERT_EQ(seq, next[t]) << "thread " << t << " out of order";
    ++next[t];
    ++total;
  }

  fclose(fp);
  EXPECT_EQ(total, kThreads * kLines);
}

//------------------------------------------------------------------------------
// With "*" and a per-file fan-out, the "*" file gets the plain main line and
// the per-file fan-out its own coloured format
//------------------------------------------------------------------------------
TEST(Logging, StarFanOutGetsMainLine)
{
  using namespace eos::common;
  FILE* star = tmpfile();
  FILE* tag = tmpfile();
  ASSERT_NE(star, nullptr);
  ASSERT_NE(tag, nullptr);
  LogBuffer* old_lb = gLogging.LB;
  const bool old_stderr = gLogging.gToStderr;
  const int old_level = gLogging.gPriorityLevel;
  const auto old_fanout = gLogging.gLogFanOut;
  LogBuffer lb;
  gLogging.LB = &lb;
  gLogging.gToStderr = false;
  gLogging.gLogFanOut.clear();
  gLogging.AddFanOut("*", star);
  gLogging.AddFanOut("LoggingTests", tag);
  gLogging.SetLogPriority(LOG_INFO);
  const std::string main_line = eos_static_log(LOG_INFO, "msg=\"fan-out test\"");
  lb.shutDown(true);
  gLogging.LB = old_lb;
  gLogging.gToStderr = old_stderr;
  gLogging.gLogFanOut = old_fanout;
  gLogging.SetLogPriority(old_level);
  char line[16384] = "";
  rewind(star);
  ASSERT_NE(fgets(line, sizeof(line), star), nullptr);
  EXPECT_EQ(std::string(line), main_line + "\n");
  rewind(tag);
  ASSERT_NE(fgets(line, sizeof(line), tag), nullptr);
  EXPECT_NE(strstr(line, "\033["), nullptr) << line;
  EXPECT_NE(strstr(line, "msg=\"fan-out test\""), nullptr) << line;
  fclose(star);
  fclose(tag);
}

//------------------------------------------------------------------------------
// Writers on different stripes and a reader that holds gMutex like the FST
// error report: the newest slot is always a complete line of the test
//------------------------------------------------------------------------------
TEST(Logging, LogMemoryReader)
{
  using namespace eos::common;
  LogBuffer* old_lb = gLogging.LB;
  const bool old_stderr = gLogging.gToStderr;
  const int old_level = gLogging.gPriorityLevel;
  const auto old_fanout = gLogging.gLogFanOut;
  LogBuffer lb;
  gLogging.LB = &lb;
  gLogging.gToStderr = false;
  gLogging.gLogFanOut.clear();
  gLogging.SetLogPriority(LOG_INFO);
  const unsigned long start = gLogging.gLogCircularIndex[LOG_INFO];
  std::vector<std::thread> threads;

  for (int t = 0; t < 32; ++t) {
    threads.emplace_back([t]() {
      for (int i = 0; i < 2000; ++i) {
        eos_static_info("msg=\"ring test\" thread=%d seq=%d", t, i);
      }
    });
  }

  int checks = 0;

  while (true) {
    gLogging.gMutex.Lock();
    const unsigned long end = gLogging.gLogCircularIndex[LOG_INFO];
    const std::string last =
        (end > start)
            ? gLogging.gLogMemory[LOG_INFO][(end - 1) % gLogging.gCircularIndexSize]
                  .c_str()
            : "";
    gLogging.gMutex.UnLock();

    if (end > start) {
      ASSERT_NE(last.find("msg=\"ring test\""), std::string::npos) << last;
      ++checks;
    }

    if (end - start == 32 * 2000) {
      break;
    }
  }

  for (auto& th : threads) {
    th.join();
  }

  lb.shutDown(true);
  gLogging.LB = old_lb;
  gLogging.gToStderr = old_stderr;
  gLogging.gLogFanOut = old_fanout;
  gLogging.SetLogPriority(old_level);
  EXPECT_EQ(gLogging.gLogCircularIndex[LOG_INFO] - start, 32u * 2000u);
  EXPECT_GT(checks, 0);
}

EOSCOMMONTESTING_END
