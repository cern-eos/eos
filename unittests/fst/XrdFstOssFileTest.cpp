//------------------------------------------------------------------------------
// File: XrdFstOssFileTest.cpp
// Author: Jozsef Makai <jmakai@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

//! Ugly hack to expose the private functions for testing
#define protected public
#define private   public
#include <fst/XrdFstOssFile.hh>
#undef protected
#undef private

#include <gtest/gtest.h>
#include <fst/tests/TestEnv.hh>
#include <XrdOuc/XrdOucTokenizer.hh>

using namespace eos::fst;
using namespace eos::fst::test;

class XrdFstOssFileTest : public ::testing::Test {
public:
    XrdFstOssFile* ossfile = nullptr;
    TestEnv* mEnv = nullptr;

    virtual void SetUp() override {
        mEnv = new eos::fst::test::TestEnv();
        ossfile = new XrdFstOssFile("test_id");
    }

    virtual void TearDown() override {
        delete ossfile;
        ossfile = nullptr;
        delete mEnv;
        mEnv = nullptr;
    }
};

TEST_F(XrdFstOssFileTest, AlignBufferTest) {
    int num_datasets = 9;
    char* ptr_off, *ptr_len;
    size_t len_req;
    off_t off_req;
    std::string str_off;
    std::string str_len;
    std::stringstream sstr;
    std::vector<XrdOucIOVec> expect_resp;

    for (int set = 1; set < num_datasets; ++set) {
        // Read in the offset and length of the request
        sstr.str("");
        sstr << "align" << set << "_off";
        off_req = (off_t)atoi(mEnv->GetMapping(sstr.str()).c_str());
        sstr.str("");
        sstr << "align" << set << "_len";
        len_req = (size_t)atoi(mEnv->GetMapping(sstr.str()).c_str());
        char* buffer = new char[len_req];
        // Read the correct answer to compare with
        sstr.str("");
        sstr << "align" << set << "_resp_off";
        str_off = mEnv->GetMapping(sstr.str());
        XrdOucTokenizer tok_off = XrdOucTokenizer((char*)str_off.c_str());
        sstr.str("");
        sstr << "align" << set << "_resp_len";
        str_len = mEnv->GetMapping(sstr.str());
        XrdOucTokenizer tok_len = XrdOucTokenizer((char*)str_len.c_str());
        ptr_off = tok_off.GetLine();
        ptr_len = tok_len.GetLine();
        expect_resp.clear();

        while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken())) {
            XrdOucIOVec expect_piece = {atol(ptr_off), atoi(ptr_len), 0, 0};
            expect_resp.push_back(expect_piece);
        }

        // Compute the alignment
        std::vector<XrdOucIOVec> resp = ossfile->AlignBuffer(buffer, off_req, len_req);
        EXPECT_EQ(expect_resp.size(), resp.size());

        for (uint32_t indx = 0; indx < resp.size(); ++indx) {
            EXPECT_EQ(expect_resp[indx].offset, resp[indx].offset);
            EXPECT_EQ(expect_resp[indx].size, resp[indx].size);
        }

        delete[] buffer;
    }
}
