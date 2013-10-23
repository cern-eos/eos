//------------------------------------------------------------------------------
// File: FileTest.cc
// Author: Elvin Sindrilaru <esindril@cern.ch> CERN
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

/*----------------------------------------------------------------------------*/
#include "FileTest.hh"
#include "common/LayoutId.hh"
#include "common/StringTokenizer.hh"
#include "fst/layout/LayoutPlugin.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/RaidMetaLayout.hh"
/*----------------------------------------------------------------------------*/

CPPUNIT_TEST_SUITE_REGISTRATION(FileTest);

//------------------------------------------------------------------------------
// setUp function
//------------------------------------------------------------------------------
void
FileTest::setUp()
{
  mEnv = new eos::fst::test::TestEnv();
}


//------------------------------------------------------------------------------
// tearDown function
//------------------------------------------------------------------------------
void
FileTest::tearDown()
{
  delete mEnv;
  mEnv = 0;
}


//------------------------------------------------------------------------------
// Vector read test
//------------------------------------------------------------------------------
void
FileTest::ReadVTest()
{
  using namespace XrdCl;
  // Initialise 
  XRootDStatus status;
  uint32_t file_size = (uint32_t)atoi(mEnv->GetMapping("file_size").c_str());
  std::string address = "root://root@" + mEnv->GetMapping("server");
  std::string file_path = mEnv->GetMapping("file_path");
  URL url(address);
  CPPUNIT_ASSERT(url.IsValid());

  std::string file_url = address + "/" + file_path;
  mFile = new File();
  status = mFile->Open(file_url, OpenFlags::Read, Access::Mode::None);
  CPPUNIT_ASSERT(status.IsOK());

  // Check that the file has the proper size
  StatInfo *stat = 0;
  status = mFile->Stat(false, stat);
  CPPUNIT_ASSERT(status.IsOK());
  CPPUNIT_ASSERT(stat);
  CPPUNIT_ASSERT(stat->GetSize() == file_size);
  CPPUNIT_ASSERT(stat->TestFlags(StatInfo::IsReadable)) ;

  // Clean up stat object
  delete stat;
  stat = 0;

  // Read the first 4KB out of each MB
  uint32_t size_chunk = 4096;
  uint32_t size_gap = 1024 *1024;
  int num_chunks = file_size / size_gap;
  char* buffer = new char[num_chunks * size_chunk];
  char* ptr_buff;
  uint64_t off;
  ChunkList readv_list;

  // Create the readv list 
  for (int i = 0; i < num_chunks; i++)
  {
    off = i * size_gap ;
    ptr_buff = buffer + i * size_chunk;
    readv_list.push_back(ChunkInfo(off, size_chunk, ptr_buff));
  }

  // Issue the readV request
  VectorReadInfo* vread_info = 0;
  status = mFile->VectorRead(readv_list, 0, vread_info);
  CPPUNIT_ASSERT(status.IsOK());
  CPPUNIT_ASSERT(vread_info->GetSize() == num_chunks * size_chunk);
  delete vread_info;
  
  // Close and delete file object
  status = mFile->Close();
  CPPUNIT_ASSERT(status.IsOK());
  delete mFile;
  mFile = 0;
}


//------------------------------------------------------------------------------
// Split vector read test
//------------------------------------------------------------------------------
void
FileTest::SplitReadVTest()
{
  using namespace eos::common;
  using namespace eos::fst;
  unsigned long layout_id = LayoutId::GetId(LayoutId::kRaid6, 1,
                                            LayoutId::kSevenStripe,
                                            LayoutId::k1M,
                                            LayoutId::kCRC32);

  RaidMetaLayout* file =  new RaidDpLayout(NULL, layout_id, NULL, NULL,
                                           eos::common::LayoutId::kXrdCl);
  
  // Create readV request
  char* buff = new char[1024*1024];
  XrdCl::ChunkList readV;
  XrdCl::ChunkList correct_rdv;
  std::ostringstream sstr;
  std::string str_off;
  std::string str_len;
  char* ptr_off, *ptr_len;
  
  // Loop through all the sets of data in the test environment
  for (int i = 1; i < 4; i++)
  {
    // Read the initial offsets
    sstr.str("");
    sstr << "off" << i;
    str_off = mEnv->GetMapping(sstr.str());
    XrdOucTokenizer tok_off = XrdOucTokenizer((char*)str_off.c_str());

    // Read the initial lengths
    sstr.str("");
    sstr << "len" << i;
    str_len = mEnv->GetMapping(sstr.str());
    XrdOucTokenizer tok_len = XrdOucTokenizer((char*)str_len.c_str());
    ptr_off = tok_off.GetLine();
    ptr_len = tok_len.GetLine();
    
    while ((ptr_off = tok_off.GetToken()) &&  (ptr_len = tok_len.GetToken()))
    {
      //std::cout << "off = " << ptr_off << " len = " << ptr_len << std::endl;
      readV.push_back(XrdCl::ChunkInfo((uint64_t)atoi(ptr_off),
                                       (uint32_t)atoi(ptr_len),
                                       (void*)0));
    }

    int indx = 0;
    std::vector<XrdCl::ChunkList> result = ((RaidMetaLayout*)file)->SplitReadV(readV);

    // Loop through the answers for each stripe and compare with the correct values
    for (auto it_stripe = result.begin(); it_stripe != result.end(); ++it_stripe)
    {
      //Read the correct answer to compare with
      sstr.str("");
      sstr << "off" << i << "_stripe" << indx;
      str_off = mEnv->GetMapping(sstr.str());
      tok_off = XrdOucTokenizer((char*)str_off.c_str());
      
      sstr.str("");
      sstr << "len" << i << "_stripe" << indx;
      str_len = mEnv->GetMapping(sstr.str());
      tok_len = XrdOucTokenizer((char*)str_len.c_str());
      
      ptr_off = tok_off.GetLine();
      ptr_len = tok_len.GetLine();
      correct_rdv.clear();
      
      while ((ptr_off = tok_off.GetToken()) &&  (ptr_len = tok_len.GetToken()))
      {
        correct_rdv.push_back(XrdCl::ChunkInfo((uint64_t)atoi(ptr_off),
                                               (uint32_t)atoi(ptr_len),
                                               buff));
      }
      
      // Test same length
      CPPUNIT_ASSERT(it_stripe->size() == correct_rdv.size());

      // Test each individual chunk
      for (auto it_chunk = it_stripe->begin(), it_resp = correct_rdv.begin();
           it_chunk != it_stripe->end(); ++it_chunk, ++it_resp)
      {
        CPPUNIT_ASSERT(it_chunk->offset == it_resp->offset);
        CPPUNIT_ASSERT(it_chunk->length == it_resp->length);          
      }
      
      indx++;
    }
    
    readV.clear();
  }

  // Free memory
  delete[] buff;
}

