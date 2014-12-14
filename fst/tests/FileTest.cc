//------------------------------------------------------------------------------
// File: FileTest.cc
// Author: Elvin Sindrilaru <esindril@cern.ch>
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
#include "fst/layout/LayoutPlugin.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
#include "fst/checksum/CRC32C.hh"
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
// Write test
//------------------------------------------------------------------------------
void
FileTest::WriteTest()
{
  using namespace XrdCl;

  // Initialise
  XRootDStatus status;
  uint32_t file_size = (uint32_t)atoi(mEnv->GetMapping("file_size").c_str());
  std::string address = "root://root@" + mEnv->GetMapping("server");
  std::string file_path = mEnv->GetMapping("plain_file");
  URL url(address);
  CPPUNIT_ASSERT(url.IsValid());
  std::string file_url = address + "/" + file_path;
  mFile = new File();
  status = mFile->Open(file_url, OpenFlags::Update | OpenFlags::Delete, Access::Mode::None);
  CPPUNIT_ASSERT(status.IsOK());

  // Write file using 4MB chunks
  uint64_t off;
  uint32_t size_chunk = 1024 * 1024;
  int num_chunks = file_size / size_chunk;
  char* buff_write = new char[size_chunk];
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(buff_write, size_chunk);
  urandom.close();

  // Create the readv list and the list for normal reads
  for (int i = 0; i < num_chunks; i++)
  {
    off = i * size_chunk ;
    status = mFile->Write(off, size_chunk, buff_write);

    if (!status.IsOK())
    {
      std::cerr << "Error while writing at off:" << off << std::endl;
      break;
    }
  }

  status = mFile->Close();
  CPPUNIT_ASSERT(status.IsOK());
  delete mFile;
  mFile = 0;
  delete[] buff_write;
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
  std::string file_path = mEnv->GetMapping("raiddp_file");
  URL url(address);
  CPPUNIT_ASSERT(url.IsValid());
  std::string file_url = address + "/" + file_path;
  mFile = new File();
  status = mFile->Open(file_url, OpenFlags::Read, Access::Mode::None);
  CPPUNIT_ASSERT(status.IsOK());

  // Check that the file has the proper size
  StatInfo* stat = 0;
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
  uint32_t size_gap = 1024 * 1024;
  int num_chunks = file_size / size_gap;
  char* buff_readv = new char[num_chunks * size_chunk];
  char* buff_read = new char[num_chunks * size_chunk];
  char* ptr_readv;
  char* ptr_read;
  uint64_t off;
  ChunkList readv_list;
  ChunkList read_list;

  // Create the readv list and the list for normal reads
  for (int i = 0; i < num_chunks; i++)
  {
    off = i * size_gap ;
    ptr_readv = buff_readv + i * size_chunk;
    ptr_read = buff_read + i * size_chunk;
    //std::cout << off << " " << size_chunk << std::endl;
    readv_list.push_back(ChunkInfo(off, size_chunk, ptr_readv));
    read_list.push_back(ChunkInfo(off, size_chunk, ptr_read));
  }

  // Issue the readV request
  uint32_t nread;
  VectorReadInfo* vread_info = 0;
  status = mFile->VectorRead(readv_list, 0, vread_info);
  CPPUNIT_ASSERT(status.IsOK());
  CPPUNIT_ASSERT(vread_info->GetSize() == num_chunks * size_chunk);
  delete vread_info;

  // Issue the normal read requests
  for (auto chunk = read_list.begin(); chunk != read_list.end(); ++chunk)
  {
    status = mFile->Read(chunk->offset, chunk->length, chunk->buffer, nread);

    if (!status.IsOK() || (nread != chunk->length))
    {
      std::cerr << "Error while reading at off:" << chunk->offset
		<< " len:" << chunk->length << std::endl;
      break;
    }
  }

  // Compute CRC32C checksum for the readv buffer and normal reads
  eos::fst::CRC32C* chksumv = new eos::fst::CRC32C();

  if (!chksumv->Add(buff_readv, num_chunks * size_chunk, 0))
  {
    std::cerr << "Checksum error: offset unaligned - skip computation" << std::endl;
    CPPUNIT_FAIL("Error computing readv checksum");
  }

  eos::fst::CRC32C* chksum = new eos::fst::CRC32C();

  if (!chksum->Add(buff_read, num_chunks * size_chunk, 0))
  {
    std::cerr << "Checksum error: offset unaligned - skip computation" << std::endl;
    CPPUNIT_FAIL("Error computing readv checksum");
  }

  std::string schksumv = chksumv->GetHexChecksum();
  std::string schksum = chksum->GetHexChecksum();
  //std::cout << "schksumv: " << schksumv << " and schksum: " << schksum << std::endl;
  CPPUNIT_ASSERT(schksumv == schksum);

  // Close and delete file object
  delete chksumv;
  delete chksum;
  status = mFile->Close();
  CPPUNIT_ASSERT(status.IsOK());
  delete mFile;
  mFile = 0;
  delete[] buff_readv;
  delete[] buff_read;
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
  int num_datasets = 4;
  char* buff = new char[1024 * 1024];
  XrdCl::ChunkList readV;
  XrdCl::ChunkList correct_rdv;
  std::ostringstream sstr;
  std::string str_off;
  std::string str_len;
  char* ptr_off, *ptr_len;

  // Loop through all the sets of data in the test environment
  for (int i = 1; i < num_datasets; i++)
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

    while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken()))
    {
      //std::cout << "off = " << ptr_off << " len = " << ptr_len << std::endl;
      readV.push_back(XrdCl::ChunkInfo((uint64_t)atoi(ptr_off),
				       (uint32_t)atoi(ptr_len),
				       (void*)0));
    }

    int indx = 0;
    std::vector<XrdCl::ChunkList> result = ((RaidMetaLayout*)file)->SplitReadV(
	readV);

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

      while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken()))
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
  delete file;
}


//------------------------------------------------------------------------------
// Test the align method used in ht XrdFstOssFile to align requests to the
// block checksum size
//------------------------------------------------------------------------------
void
FileTest::AlignBufferTest()
{
  using namespace eos::fst;
  int num_datasets = 8;
  char* ptr_off, *ptr_len;
  size_t len_req;
  off_t off_req;
  std::string str_off;
  std::string str_len;
  std::stringstream sstr;
  XrdFstOssFile* ossfile = new XrdFstOssFile("test_id");
  std::vector<XrdOucIOVec> expect_resp;

  for (int set = 1; set < num_datasets; ++set)
  {
    // Read in the offset and legth of the request
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

    while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken()))
    {
      XrdOucIOVec expect_piece = {atol(ptr_off), atoi(ptr_len), 0, 0};
      expect_resp.push_back(expect_piece);
    }

    // Compute the alignment
    std::vector<XrdOucIOVec> resp = ossfile->AlignBuffer(buffer, off_req, len_req);
    //std::cout << "set:" << set
    //          << " size exp:" << expect_resp.size()
    //          << " size resp:" << resp.size() << std::endl;
    CPPUNIT_ASSERT(resp.size() == expect_resp.size());

    for (uint32_t indx = 0; indx < resp.size(); ++indx)
    {
      //std::cout << "resp off:" << resp[indx].offset
      //          << " len:" << resp[indx].size << std::endl;
      //std::cout << "exp off:" << expect_resp[indx].offset
      //          << " len:" << expect_resp[indx].size << std::endl;
      CPPUNIT_ASSERT(resp[indx].offset == expect_resp[indx].offset);
      CPPUNIT_ASSERT(resp[indx].size == expect_resp[indx].size);
    }

    delete[] buffer;
  }

  delete ossfile;
}


//----------------------------------------------------------------------------
//! Test the deletion of a file to which the delete flag is sent using the
//! fctl function on the file object
//----------------------------------------------------------------------------
void
FileTest::DeleteFlagTest()
{
  using namespace XrdCl;

  // Fill buffer with random characters
  uint64_t offset = 0;
  uint32_t block_size = 4 * 1024;
  char* buffer = new char[block_size];
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(buffer, block_size);
  urandom.close();

  // Initialise
  XRootDStatus status;
  std::string address = "root://root@" + mEnv->GetMapping("server");
  std::string file_path = mEnv->GetMapping("dummy_file");
  URL url(address);
  CPPUNIT_ASSERT(url.IsValid());
  std::string file_url = address + "/" + file_path;
  mFile = new File();
  status = mFile->Open(file_url, OpenFlags::Delete | OpenFlags::Update,
		       Access::Mode::UR | Access::Mode::UW);
  CPPUNIT_ASSERT(status.IsOK());

  // Write some data to the file
  for (uint32_t i = 0; i < 10; i++)
  {
    status = mFile->Write(offset, block_size, buffer);
    offset += block_size;
    CPPUNIT_ASSERT(status.IsOK());
  }

  // Send the delete command using Fcntl
  Buffer arg;
  Buffer* response;
  arg.FromString("delete");
  status = mFile->Fcntl(arg, response);
  CPPUNIT_ASSERT(status.IsOK());
  delete response;

  // Close the file and then test for its existance
  status = mFile->Close();
  status = mFile->Open(file_url, OpenFlags::Read, Access::Mode::None);
  CPPUNIT_ASSERT(!status.IsOK());
  delete[] buffer;
  delete mFile;
  mFile = 0;
}
