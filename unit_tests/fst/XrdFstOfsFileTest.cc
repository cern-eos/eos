//------------------------------------------------------------------------------
//! @file XrdFstOfsFileTest.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#include "TestEnv.hh"
#include "fst/layout/RaidMetaLayout.hh"
#include "fst/checksum/CRC32C.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/io/xrd/XrdIo.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include <string>

EOSFSTTEST_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Write test - not useful at the moment
//------------------------------------------------------------------------------
TEST(FstFileTest, DISABLED_WriteTest)
{
  using namespace XrdCl;
  std::unique_ptr<File> mFile(new File());
  // Initialize
  uint32_t file_size = (uint32_t) atoi(gEnv->GetMapping("file_size").c_str());
  std::string address = "root://root@" + gEnv->GetMapping("server");
  std::string file_path = gEnv->GetMapping("replica_file");
  // Validate URL
  URL url(address);
  ASSERT_TRUE(url.IsValid());
  // Open file
  std::string file_url = address + "/" + file_path;
  GLOG << "Opening file: " << file_url << std::endl;
  XRootDStatus status = mFile->Open(file_url,
                                    OpenFlags::Update | OpenFlags::Delete,
                                    Access::Mode::None);
  ASSERT_TRUE(status.IsOK());
  // Prepare 1MB chunk
  uint32_t size_chunk = 1024 * 1024;
  int num_chunks = file_size / size_chunk;
  char* buff_write = new char[size_chunk];
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(buff_write, size_chunk);
  urandom.close();
  // Write the file in 1MB chunks
  GLOG << "Performing write operation" << std::endl;

  for (int i = 0; i < num_chunks; i++) {
    uint64_t offset = (uint64_t) i * size_chunk ;
    status = mFile->Write(offset, size_chunk, buff_write);

    if (!status.IsOK()) {
      std::cerr << "Error while writing at offset:" << offset << std::endl;
      std::terminate();
    }
  }

  // Close file
  status = mFile->Close();
  ASSERT_TRUE(status.IsOK());
  delete[] buff_write;
}

//------------------------------------------------------------------------------
// Vector read test
//------------------------------------------------------------------------------
TEST(FstFileTest, ReadVTest)
{
  using namespace XrdCl;
  std::unique_ptr<File> mFile(new File());
  uint32_t nread;
  // Initialize
  uint32_t file_size = (uint32_t) atoi(gEnv->GetMapping("file_size").c_str());
  std::string address = "root://root@" + gEnv->GetMapping("server");
  std::string file_path = gEnv->GetMapping("raiddp_file");
  // Validate URL
  URL url(address);
  ASSERT_TRUE(url.IsValid());
  // Open file
  std::string file_url = address + "/" + file_path;
  GLOG << "Opening file: " << file_url << std::endl;
  XRootDStatus status = mFile->Open(file_url, OpenFlags::Read,
                                    Access::Mode::None);
  ASSERT_TRUE(status.IsOK());
  // Check file has the proper size
  StatInfo* stat = nullptr;
  status = mFile->Stat(false, stat);
  ASSERT_TRUE(status.IsOK());
  GLOG << "Stat size: " << stat->GetSize() << std::endl;
  ASSERT_EQ(stat->GetSize(), file_size);
  delete stat;
  // Read the first 4KB out of each MB
  uint32_t size_chunk = 4096;
  uint32_t size_gap = 1024 * 1024;
  int num_chunks = file_size / size_gap;
  char* ptr_readv;
  char* ptr_read;
  char* buff_readv = new char[num_chunks * size_chunk];
  char* buff_read = new char[num_chunks * size_chunk];
  ChunkList readv_list;
  ChunkList read_list;

  // Create the readV list and the list for normal reads
  for (int i = 0; i < num_chunks; i++) {
    uint64_t off = (uint64_t) i * size_gap ;
    ptr_readv = buff_readv + i * size_chunk;
    readv_list.push_back(ChunkInfo(off, size_chunk, ptr_readv));
    ptr_read = buff_read + i * size_chunk;
    read_list.push_back(ChunkInfo(off, size_chunk, ptr_read));
  }

  // Issue the readV request
  GLOG << "Performing readV operation" << std::endl;
  VectorReadInfo* vread_info = nullptr;
  status = mFile->VectorRead(readv_list, 0, vread_info);
  ASSERT_TRUE(status.IsOK());
  ASSERT_EQ(num_chunks * size_chunk, vread_info->GetSize());
  delete vread_info;
  // Issue the normal read requests
  GLOG << "Performing normal read operation" << std::endl;

  for (auto chunk = read_list.begin(); chunk != read_list.end(); ++chunk) {
    status = mFile->Read(chunk->offset, chunk->length, chunk->buffer, nread);

    if (!status.IsOK() || (nread != chunk->length)) {
      std::cerr << "Error while reading at offset:" << chunk->offset
                << " len:" << chunk->length << std::endl;
      std::terminate();
    }
  }

  // Compute CRC32C checksum for the readV buffer
  eos::fst::CRC32C* chksumv = new eos::fst::CRC32C();

  if (!chksumv->Add(buff_readv, num_chunks * size_chunk, 0)) {
    std::cerr << "Checksum error: offset unaligned - skip computation" << std::endl;
    std::terminate();
  }

  // Compute CRC32C checksum for the normal read buffer
  eos::fst::CRC32C* chksum = new eos::fst::CRC32C();

  if (!chksum->Add(buff_read, num_chunks * size_chunk, 0)) {
    std::cerr << "Checksum error: offset unaligned - skip computation" << std::endl;
    std::terminate();
  }

  // Compare checksums
  std::string schksumv = chksumv->GetHexChecksum();
  std::string schksum = chksum->GetHexChecksum();
  GLOG << "ChecksumV: " << schksumv << std::endl;
  GLOG << "Checksum:  " << schksum << std::endl;
  ASSERT_STREQ(schksum.c_str(), schksumv.c_str());
  // Close file
  status = mFile->Close();
  ASSERT_TRUE(status.IsOK());
  delete[] buff_readv;
  delete[] buff_read;
}

//------------------------------------------------------------------------------
// Split vector read test
//------------------------------------------------------------------------------
TEST(FstFileTest, SplitReadVTest)
{
  using namespace eos::common;
  using namespace eos::fst;
  unsigned long layout_id = LayoutId::GetId(LayoutId::kRaid6, 1,
                            6,
                            LayoutId::k1M,
                            LayoutId::kCRC32);
  std::unique_ptr<RaidDpLayout> file(new RaidDpLayout(NULL, layout_id, NULL, NULL,
                                     "root://localhost//dummy"));
  // Create readV request
  int num_datasets = 4;
  char* buff = new char[1024 * 1024];
  XrdCl::ChunkList readV;
  XrdCl::ChunkList correct_rdv;
  std::ostringstream sstr;
  std::string str_off;
  std::string str_len;
  char* ptr_off;
  char* ptr_len;

  // Loop through all the sets of data in the test environment
  for (int i = 1; i < num_datasets; i++) {
    // Read the initial offsets
    sstr.str("");
    sstr << "off" << i;
    str_off = gEnv->GetMapping(sstr.str());
    XrdOucTokenizer tok_off = XrdOucTokenizer((char*) str_off.c_str());
    // Read the initial lengths
    sstr.str("");
    sstr << "len" << i;
    str_len = gEnv->GetMapping(sstr.str());
    XrdOucTokenizer tok_len = XrdOucTokenizer((char*) str_len.c_str());
    ptr_off = tok_off.GetLine();
    ptr_len = tok_len.GetLine();

    while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken())) {
      readV.push_back(XrdCl::ChunkInfo((uint64_t) atoi(ptr_off),
                                       (uint32_t) atoi(ptr_len),
                                       (void*) 0));
    }

    int indx = 0;
    std::vector<XrdCl::ChunkList> result = ((RaidMetaLayout*)
                                            file.get())->SplitReadV(readV);

    // Loop through the answers for each stripe and compare with the correct values
    for (auto it_stripe = result.begin(); it_stripe != result.end(); ++it_stripe) {
      //Read the correct answer to compare with
      sstr.str("");
      sstr << "off" << i << "_stripe" << indx;
      str_off = gEnv->GetMapping(sstr.str());
      tok_off = XrdOucTokenizer((char*)str_off.c_str());
      sstr.str("");
      sstr << "len" << i << "_stripe" << indx;
      str_len = gEnv->GetMapping(sstr.str());
      tok_len = XrdOucTokenizer((char*)str_len.c_str());
      ptr_off = tok_off.GetLine();
      ptr_len = tok_len.GetLine();
      correct_rdv.clear();

      while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken())) {
        correct_rdv.push_back(XrdCl::ChunkInfo((uint64_t) atoi(ptr_off),
                                               (uint32_t) atoi(ptr_len),
                                               buff));
      }

      // Test same length
      ASSERT_EQ(correct_rdv.size(), it_stripe->size());

      // Test each individual chunk
      for (auto it_chunk = it_stripe->begin(), it_resp = correct_rdv.begin();
           it_chunk != it_stripe->end(); ++it_chunk, ++it_resp) {
        ASSERT_EQ(it_resp->offset, it_chunk->offset);
        ASSERT_EQ(it_resp->length, it_chunk->length);
      }

      indx++;
    }

    readV.clear();
  }

  // Free memory
  delete[] buff;
}

//----------------------------------------------------------------------------
//! Test the deletion of a file to which the delete flag is sent using the
//! Fcntl function on the file object
//----------------------------------------------------------------------------
TEST(FstFileTest, DeleteFlagTest)
{
  using namespace XrdCl;
  std::unique_ptr<File> mFile(new File());
  // Initialize
  std::string address = "root://root@" + gEnv->GetMapping("server");
  std::string file_path = gEnv->GetMapping("dummy_file");
  // Validate URL
  URL url(address);
  ASSERT_TRUE(url.IsValid());
  // Open file
  std::string file_url = address + "/" + file_path;
  GLOG << "Opening file: " << file_url << std::endl;
  XRootDStatus status = mFile->Open(file_url,
                                    OpenFlags::Delete | OpenFlags::Update,
                                    Access::Mode::UR | Access::Mode::UW);
  ASSERT_TRUE(status.IsOK());
  // Fill buffer with random characters
  uint32_t block_size = 4 * 1024;
  char* buffer = new char[block_size];
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(buffer, block_size);
  urandom.close();
  // Write data to the file
  GLOG << "Performing write operation" << std::endl;

  for (int i = 0; i < 10; i++) {
    uint64_t offset = (uint64_t) i * block_size;
    status = mFile->Write(offset, block_size, buffer);

    if (!status.IsOK()) {
      std::cerr << "Error while writing at offset: " << offset << std::endl;
      std::terminate();
    }
  }

  // Send the delete command using Fcntl
  XrdCl::Buffer* response;
  XrdCl::Buffer arg;
  arg.FromString("delete");
  GLOG << "Sending delete command using Fcntl" << std::endl;
  status = mFile->Fcntl(arg, response);
  ASSERT_TRUE(status.IsOK());
  delete response;
  // Close the file and then test for its existence
  GLOG << "Attempt to reopen deleted file" << std::endl;
  status = mFile->Close();
  ASSERT_FALSE(status.IsOK());
  status = mFile->Open(file_url, OpenFlags::Read, Access::Mode::None);
  ASSERT_FALSE(status.IsOK());
  delete[] buffer;
}

//------------------------------------------------------------------------------
// Read async test
//------------------------------------------------------------------------------
TEST(FstFileTest, ReadAsyncTest)
{
  // Initialize
  std::string address = "root://root@" + gEnv->GetMapping("server");
  std::string file_path = gEnv->GetMapping("replica_file");
  // Validate URL
  XrdCl::URL url(address);
  ASSERT_TRUE(url.IsValid());
  // Open file
  std::string file_url = address + "/" + file_path;
  GLOG << "Opening file: " << file_url << std::endl;
  std::unique_ptr<eos::fst::XrdIo> file(new eos::fst::XrdIo(file_url));
  ASSERT_EQ(file->fileOpen(SFS_O_RDONLY), 0);
  // Get file size
  struct stat buff = {0};
  ASSERT_EQ(file->fileStat(&buff), 0);
  GLOG << "Stat size: " << buff.st_size << std::endl;
  uint64_t file_size = (uint64_t) buff.st_size;
  off_t buff_size = 4 * 1024;
  char* buffer = new char[buff_size];
  uint64_t offset = 0;
  GLOG << "Performing async read operation" << std::endl;

  while (offset < file_size) {
    off_t read_size = file->fileReadAsync(offset, buffer, buff_size, false);
    ASSERT_EQ(buff_size, read_size);
    offset += buff_size;
  }

  off_t read_size = file->fileReadAsync(offset, buffer, buff_size, true);
  ASSERT_EQ(read_size, 0);
  ASSERT_EQ(file->fileClose(), 0);
  delete[] buffer;
}


//------------------------------------------------------------------------------
// Read async test
//------------------------------------------------------------------------------
TEST(FstFileTest, ReadAsyncTestRA)
{
  // Initialize
  std::string address = "root://root@" + gEnv->GetMapping("server");
  std::string file_path = gEnv->GetMapping("replica_file");
  // Validate URL
  XrdCl::URL url(address);
  ASSERT_TRUE(url.IsValid());
  // Open file
  std::string ra = "?fst.readahead=true";
  std::string file_url = address + "/" + file_path + ra;
  GLOG << "Opening file: " << file_url << std::endl;
  std::unique_ptr<eos::fst::XrdIo> file(new eos::fst::XrdIo(file_url));
  ASSERT_EQ(file->fileOpen(SFS_O_RDONLY, 0), 0);
  // Get file size
  struct stat buff = {0};
  ASSERT_EQ(file->fileStat(&buff), 0);
  GLOG << "Stat size: " << buff.st_size << std::endl;
  uint64_t file_size = (uint64_t) buff.st_size;
  off_t buff_size = 4 * 1024;
  char* buffer = new char[buff_size];
  uint64_t offset = 0;
  GLOG << "Performing async read operation" << std::endl;

  while (offset < file_size) {
    off_t read_size = file->fileReadAsync(offset, buffer, buff_size, true);
    ASSERT_EQ(buff_size, read_size);
    offset += buff_size;
  }

  off_t read_size = file->fileReadAsync(offset, buffer, buff_size, true);
  ASSERT_EQ(read_size, 0);
  ASSERT_EQ(file->fileClose(), 0);
  delete[] buffer;
}

EOSFSTTEST_NAMESPACE_END
