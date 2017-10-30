//------------------------------------------------------------------------------
// File: environment-reader.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include <gtest/gtest.h>
#include "auth/EnvironmentReader.hh"

TEST(EnvironmentReader, BasicSanity) {
  EnvironmentReader reader;
  reader.launchWorkers(3);

  Environment env1;
  env1.fromVector({"KEY1=VALUE1", "KEY2=VALUE2", "KEY3=VALUE3", "KEY4=VALUE4"});

  Environment env2;
  env2.fromVector({"KRB5CCNAME=FILE:/tmp/krb-cache"});

  reader.inject(3, env1, std::chrono::milliseconds(0));
  reader.inject(4, env2, std::chrono::milliseconds(10));
  reader.inject(1, env1, std::chrono::milliseconds(30));
  reader.inject(3978, env2, std::chrono::milliseconds(1));

  EnvironmentResponse response1 = reader.stageRequest(1);
  EnvironmentResponse response1_2 = reader.stageRequest(1);
  EnvironmentResponse response1_3 = reader.stageRequest(1);
  EnvironmentResponse response3 = reader.stageRequest(3);
  EnvironmentResponse response4 = reader.stageRequest(4);
  EnvironmentResponse response3978 = reader.stageRequest(3978);

  ASSERT_EQ(response1.contents.get(), env1);
  ASSERT_EQ(response1_2.contents.get(), env1);
  ASSERT_EQ(response1_3.contents.get(), env1);
  ASSERT_EQ(response1.queuedSince, response1_2.queuedSince);
  ASSERT_EQ(response1.queuedSince, response1_3.queuedSince);

  ASSERT_EQ(response3.contents.get(), env1);
  ASSERT_EQ(response4.contents.get(), env2);
  ASSERT_EQ(response3978.contents.get(), env2);
}

static void inject(EnvironmentReader &reader, size_t from, size_t until) {
  for(size_t i = from; i < until; i++) {
    Environment env;

    if(!(i % 150)) {
      env.fromVector( {SSTR("Key" << i << "=Value" << i)});
      reader.inject(i, env, std::chrono::milliseconds(i % 3));
    }
  }
}

static void issueRequests(EnvironmentReader &reader, size_t from, size_t until) {
  std::vector<EnvironmentResponse> responses;
  for(size_t i = from; i < until; i++) {
    responses.emplace_back(reader.stageRequest(i));
  }

  for(size_t i = from; i < until; i++) {
    Environment expected;

    if(!(i % 150)) {
      expected.fromVector( {SSTR("Key" << i << "=Value" << i)});
    }
    ASSERT_EQ(responses[i-from].contents.get(), expected) << i;
  }
}

TEST(EnvironmentReader, HeavyLoad) {
  EnvironmentReader reader;
  reader.launchWorkers(30);

  inject(reader, 0, 10000);

  // 10 threads * 2, 1000 requests each
  const size_t nthreads = 10;

  std::vector<std::thread> threads;
  for(size_t i = 0; i < nthreads; i++) {
    threads.emplace_back(issueRequests, std::ref(reader), (i*1000), (i+1)*1000);
    threads.emplace_back(issueRequests, std::ref(reader), (i*1000), (i+1)*1000);
  }

  for(size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
}
