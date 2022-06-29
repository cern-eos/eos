//------------------------------------------------------------------------------
//! @file ConvertZMQTests.cc
//! @author Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include "common/ShellCmd.hh"
#include "mgm/convert/ConversionZMQ.cc"
#include "common/StringConversion.hh"
#include <future>


//------------------------------------------------------------------------------
// Test ZMQ driven TPC
//------------------------------------------------------------------------------
TEST(ConversionPool, Functional)
{
  using namespace eos::mgm;
  pid_t xrd;
  if (!(xrd = fork())) {
    system("/opt/eos/xrootd/bin/xrootd -Rdaemon -p 21235 -n conversiontest -d >& /tmp/conversion.xrootd.log");
    exit(0);
  }
  std::unique_ptr<ConversionZMQ> conv(new ConversionZMQ(16 ,6000, false));
  conv->RunServer();
  conv->SetupClients();
  ASSERT_TRUE(conv);

  system("mkdir -p /tmp/conversiontest/");
  system("cp /etc/passwd /tmp/conversiontest/source");
  system("ls -la /tmp/conversiontest/");

  struct stat buf;

  ::stat("/tmp/conversiontest/source", &buf);
  size_t source_size = buf.st_size;

  for (size_t i = 0 ; i<10; ++i) {
    std::string transfer="10|XrdSecDEBUG=1|root://localhost:21235//tmp/conversiontest/source|root://localhost:21235//tmp/conversiontest/target.";
    transfer += std::to_string(i);
    int ret = conv->Send(transfer);
    ASSERT_EQ(ret, 0);
    std::string target = "/tmp/conversiontest/target.";
    target += std::to_string(i);
    ::stat(target.c_str(), &buf);
    ASSERT_EQ(buf.st_size, source_size);
  }


  // CREATE A TIMEOUT

  {
    std::string transfer="2||root://localhost:60000//dummy1|root://localhost:60000//dummy2";
    int ret = conv->Send(transfer);
    ASSERT_EQ(ret, 110); // ETIMEDOUT
  }


  std::vector<std::future<int>> transferResult;


  for (size_t i = 0 ; i < 200; ++i) {
    std::string transfer="10|PATH=/opt/eos/xrootd/bin:$PATH|root://localhost:21235//tmp/conversiontest/source|root://localhost:21235//tmp/conversiontest/target.async";
    transfer += std::to_string(i);

    transferResult.push_back(std::async(std::launch::async, [&conv](std::string t) {
	return conv->Send(t);
      }, transfer)
      );
  }
  for (size_t i = 0 ; i < 200; ++i) {
    int ret = transferResult[i].get();
    ASSERT_EQ(ret, 0);
    std::string target = "/tmp/conversiontest/target.async";
    target += std::to_string(i);
    ::stat(target.c_str(), &buf);
    ASSERT_EQ(buf.st_size, source_size);
  }
 
  system("ls -la /tmp/conversiontest/");
  system("rm -rf /tmp/conversiontest/*");
  system("pkill -f \"/opt/eos/xrootd/bin/xrootd -Rdaemon -p 21235 -n conversiontest -d\"");
}

