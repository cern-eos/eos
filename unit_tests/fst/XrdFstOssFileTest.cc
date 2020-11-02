//------------------------------------------------------------------------------
// File: XrdFstOssFileTest.cc
// Author: Jozsef Makai <jmakai@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#define IN_TEST_HARNESS
#include "fst/XrdFstOssFile.hh"
#undef IN_TEST_HARNESS

#include "gtest/gtest.h"
#include "unit_tests/fst/TestEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"

using namespace eos::fst;
using namespace eos::fst::test;

class XrdFstOssFileTest : public ::testing::Test
{
public:
  XrdFstOssFile* ossfile = nullptr;
  std::map<std::string, std::string> mMapParam;

  virtual void SetUp() override
  {
    // Test sequences for the AlingBuffer method
    // Test set 1
    mMapParam.insert(std::make_pair("align1_off", "4095"));
    mMapParam.insert(std::make_pair("align1_len", "8194"));
    mMapParam.insert(std::make_pair("align1_resp_off", "0, 4096, 12288"));
    mMapParam.insert(std::make_pair("align1_resp_len", "4096, 8192, 4096"));
    // Test set 2
    mMapParam.insert(std::make_pair("align2_off", "4095"));
    mMapParam.insert(std::make_pair("align2_len", "1048576"));
    mMapParam.insert(std::make_pair("align2_resp_off", "0 4096 1048576"));
    mMapParam.insert(std::make_pair("align2_resp_len", "4096 1044480 4096"));
    // Test set 3
    mMapParam.insert(std::make_pair("align3_off", "4096"));
    mMapParam.insert(std::make_pair("align3_len", "1048576"));
    mMapParam.insert(std::make_pair("align3_resp_off", "4096"));
    mMapParam.insert(std::make_pair("align3_resp_len", "1048576"));
    // Test set 4
    mMapParam.insert(std::make_pair("align4_off", "20971520"));
    mMapParam.insert(std::make_pair("align4_len", "2048"));
    mMapParam.insert(std::make_pair("align4_resp_off", "20971520"));
    mMapParam.insert(std::make_pair("align4_resp_len", "4096"));
    // Test set 5
    mMapParam.insert(std::make_pair("align5_off", "20972544"));
    mMapParam.insert(std::make_pair("align5_len", "3072"));
    mMapParam.insert(std::make_pair("align5_resp_off", "20971520"));
    mMapParam.insert(std::make_pair("align5_resp_len", "4096"));
    // Test set 6
    mMapParam.insert(std::make_pair("align6_off", "20972544"));
    mMapParam.insert(std::make_pair("align6_len", "4096"));
    mMapParam.insert(std::make_pair("align6_resp_off", "20971520 20975616"));
    mMapParam.insert(std::make_pair("align6_resp_len", "4096 4096"));
    // Test set 7
    mMapParam.insert(std::make_pair("align7_off", "20972544"));
    mMapParam.insert(std::make_pair("align7_len", "9216"));
    mMapParam.insert(std::make_pair("align7_resp_off",
                                    "20971520 20975616 20979712"));
    mMapParam.insert(std::make_pair("align7_resp_len", "4096 4096 4096"));
    // Test set 8
    mMapParam.insert(std::make_pair("align8_off", "10"));
    mMapParam.insert(std::make_pair("align8_len", "1025"));
    mMapParam.insert(std::make_pair("align8_resp_off", "0"));
    mMapParam.insert(std::make_pair("align8_resp_len", "4096"));
    ossfile = new XrdFstOssFile("test_id");
  }

  virtual void TearDown() override
  {
    delete ossfile;
    ossfile = nullptr;
  }
};

TEST_F(XrdFstOssFileTest, AlignBufferTest)
{
  int num_datasets = 9;
  char* ptr_off, *ptr_len;
  size_t len_req;
  off_t off_req;
  std::string str_off;
  std::string str_len;
  std::stringstream sstr;
  std::vector<XrdOucIOVec> expect_resp;
  std::shared_ptr<eos::common::Buffer> start_piece, end_piece;

  for (int set = 1; set < num_datasets; ++set) {
    // Read in the offset and length of the request
    sstr.str("");
    sstr << "align" << set << "_off";
    off_req = (off_t) atoi(mMapParam[sstr.str()].c_str());
    sstr.str("");
    sstr << "align" << set << "_len";
    len_req = (size_t) atoi(mMapParam[sstr.str()].c_str());
    char* buffer = new char[len_req];
    // Read the correct answer to compare with
    sstr.str("");
    sstr << "align" << set << "_resp_off";
    str_off = mMapParam[sstr.str()];
    XrdOucTokenizer tok_off = XrdOucTokenizer((char*) str_off.c_str());
    sstr.str("");
    sstr << "align" << set << "_resp_len";
    str_len = mMapParam[sstr.str()];
    XrdOucTokenizer tok_len = XrdOucTokenizer((char*) str_len.c_str());
    ptr_off = tok_off.GetLine();
    ptr_len = tok_len.GetLine();
    expect_resp.clear();

    while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken())) {
      XrdOucIOVec expect_piece = {atol(ptr_off), atoi(ptr_len), 0, 0};
      expect_resp.push_back(expect_piece);
    }

    // Compute the alignment
    std::vector<XrdOucIOVec> resp =
      ossfile->AlignBuffer(buffer, off_req, len_req, start_piece, end_piece);
    EXPECT_EQ(expect_resp.size(), resp.size());

    for (uint32_t indx = 0; indx < resp.size(); ++indx) {
      EXPECT_EQ(expect_resp[indx].offset, resp[indx].offset);
      EXPECT_EQ(expect_resp[indx].size, resp[indx].size);
    }

    delete[] buffer;
  }
}
