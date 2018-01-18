//------------------------------------------------------------------------------
// File: FstFileTest.cc
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

#include "FstFileTest.hh"
#include "TestEnv.hh"
#include "fst/layout/RaidMetaLayout.hh"
#include "fst/checksum/CRC32C.hh"
#include "fst/layout/RaidDpLayout.hh"
#include <XrdOuc/XrdOucTokenizer.hh>

EOSFSTTEST_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Write test
//------------------------------------------------------------------------------
void
WriteTest()
{
  using namespace XrdCl;
  std::unique_ptr<File> mFile(new File());
  std::unique_ptr<eos::fst::test::TestEnv> mEnv(new eos::fst::test::TestEnv());
  // Initialise
  XRootDStatus status;
  uint32_t file_size = (uint32_t)atoi(mEnv->GetMapping("file_size").c_str());
  std::string address = "root://root@" + mEnv->GetMapping("server");
  std::string file_path = mEnv->GetMapping("plain_file");
  URL url(address);
  assert(url.IsValid());
  std::string file_url = address + "/" + file_path;
  status = mFile->Open(file_url, OpenFlags::Update | OpenFlags::Delete,
                       Access::Mode::None);
  assert(status.IsOK());
  // Write file using 4MB chunks
  uint64_t off;
  uint32_t size_chunk = 1024 * 1024;
  int num_chunks = file_size / size_chunk;
  char* buff_write = new char[size_chunk];
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(buff_write, size_chunk);
  urandom.close();

  // Create the readv list and the list for normal reads
  for (int i = 0; i < num_chunks; i++) {
    off = (uint64_t) i * size_chunk ;
    status = mFile->Write(off, size_chunk, buff_write);

    if (!status.IsOK()) {
      std::cerr << "Error while writing at off:" << off << std::endl;
      break;
    }
  }

  status = mFile->Close();
  assert(status.IsOK());
  delete[] buff_write;
}

//------------------------------------------------------------------------------
// Vector read test
//------------------------------------------------------------------------------
void
ReadVTest()
{
  using namespace XrdCl;
  std::unique_ptr<File> mFile(new File());
  std::unique_ptr<eos::fst::test::TestEnv> mEnv(new eos::fst::test::TestEnv());
  // Initialise
  XRootDStatus status;
  uint32_t file_size = (uint32_t)atoi(mEnv->GetMapping("file_size").c_str());
  std::string address = "root://root@" + mEnv->GetMapping("server");
  std::string file_path = mEnv->GetMapping("raiddp_file");
  URL url(address);
  assert(url.IsValid());
  std::string file_url = address + "/" + file_path;
  status = mFile->Open(file_url, OpenFlags::Read, Access::Mode::None);
  assert(status.IsOK());
  // Check that the file has the proper size
  StatInfo* stat = nullptr;
  status = mFile->Stat(false, stat);
  assert(status.IsOK());
  assert(stat);
  assert(stat->GetSize() == file_size);
  assert(stat->TestFlags(StatInfo::IsReadable)) ;
  // Clean up stat object
  delete stat;
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
  for (int i = 0; i < num_chunks; i++) {
    off = (uint64_t) i * size_gap ;
    ptr_readv = buff_readv + i * size_chunk;
    ptr_read = buff_read + i * size_chunk;
    readv_list.push_back(ChunkInfo(off, size_chunk, ptr_readv));
    read_list.push_back(ChunkInfo(off, size_chunk, ptr_read));
  }

  // Issue the readV request
  uint32_t nread;
  VectorReadInfo* vread_info = nullptr;
  status = mFile->VectorRead(readv_list, 0, vread_info);
  assert(status.IsOK());
  assert(num_chunks * size_chunk == vread_info->GetSize());
  delete vread_info;

  // Issue the normal read requests
  for (auto chunk = read_list.begin(); chunk != read_list.end(); ++chunk) {
    status = mFile->Read(chunk->offset, chunk->length, chunk->buffer, nread);

    if (!status.IsOK() || (nread != chunk->length)) {
      std::cerr << "Error while reading at off:" << chunk->offset
                << " len:" << chunk->length << std::endl;
      std::terminate();
      break;
    }
  }

  // Compute CRC32C checksum for the readv buffer and normal reads
  eos::fst::CRC32C* chksumv = new eos::fst::CRC32C();

  if (!chksumv->Add(buff_readv, num_chunks * size_chunk, 0)) {
    std::cerr << "Checksum error: offset unaligned - skip computation" << std::endl;
    std::terminate();
  }

  eos::fst::CRC32C* chksum = new eos::fst::CRC32C();

  if (!chksum->Add(buff_read, num_chunks * size_chunk, 0)) {
    std::cerr << "Checksum error: offset unaligned - skip computation" << std::endl;
    std::terminate();
  }

  std::string schksumv = chksumv->GetHexChecksum();
  std::string schksum = chksum->GetHexChecksum();
  assert(schksum.compare(schksumv));
  // Close and delete file object
  delete chksumv;
  delete chksum;
  status = mFile->Close();
  assert(status.IsOK());
  delete[] buff_readv;
  delete[] buff_read;
}

//------------------------------------------------------------------------------
// Split vector read test
//------------------------------------------------------------------------------
void
SplitReadVTest()
{
  using namespace eos::common;
  using namespace eos::fst;
  unsigned long layout_id = LayoutId::GetId(LayoutId::kRaid6, 1,
                            LayoutId::kSevenStripe,
                            LayoutId::k1M,
                            LayoutId::kCRC32);
  std::unique_ptr<RaidDpLayout> file(new RaidDpLayout(NULL, layout_id, NULL, NULL,
                                     "root://localhost//dummy"));
  std::unique_ptr<eos::fst::test::TestEnv> mEnv(new eos::fst::test::TestEnv());
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
  for (int i = 1; i < num_datasets; i++) {
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

    while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken())) {
      readV.push_back(XrdCl::ChunkInfo((uint64_t)atoi(ptr_off),
                                       (uint32_t)atoi(ptr_len),
                                       (void*)0));
    }

    int indx = 0;
    std::vector<XrdCl::ChunkList> result = ((RaidMetaLayout*)
                                            file.get())->SplitReadV(
                                                readV);

    // Loop through the answers for each stripe and compare with the correct values
    for (auto it_stripe = result.begin(); it_stripe != result.end(); ++it_stripe) {
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

      while ((ptr_off = tok_off.GetToken()) && (ptr_len = tok_len.GetToken())) {
        correct_rdv.push_back(XrdCl::ChunkInfo((uint64_t)atoi(ptr_off),
                                               (uint32_t)atoi(ptr_len),
                                               buff));
      }

      // Test same length
      assert(correct_rdv.size() == it_stripe->size());

      // Test each individual chunk
      for (auto it_chunk = it_stripe->begin(), it_resp = correct_rdv.begin();
           it_chunk != it_stripe->end(); ++it_chunk, ++it_resp) {
        assert(it_resp->offset == it_chunk->offset);
        assert(it_resp->length == it_chunk->length);
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
//! fctl function on the file object
//----------------------------------------------------------------------------
void
DeleteFlagTest()
{
  using namespace XrdCl;
  std::unique_ptr<File> mFile(new File());
  std::unique_ptr<eos::fst::test::TestEnv> mEnv(new eos::fst::test::TestEnv());
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
  assert(url.IsValid());
  std::string file_url = address + "/" + file_path;
  status = mFile->Open(file_url, OpenFlags::Delete | OpenFlags::Update,
                       Access::Mode::UR | Access::Mode::UW);
  assert(status.IsOK());

  // Write some data to the file
  for (uint32_t i = 0; i < 10; i++) {
    status = mFile->Write(offset, block_size, buffer);
    offset += block_size;
    assert(status.IsOK());
  }

  // Send the delete command using Fcntl
  XrdCl::Buffer arg;
  XrdCl::Buffer* response;
  arg.FromString("delete");
  status = mFile->Fcntl(arg, response);
  assert(status.IsOK());
  delete response;
  // Close the file and then test for its existence
  status = mFile->Close();
  assert(!status.IsOK());
  status = mFile->Open(file_url, OpenFlags::Read, Access::Mode::None);
  assert(!status.IsOK());
  delete[] buffer;
}

//------------------------------------------------------------------------------
// Read async test
//------------------------------------------------------------------------------
void
ReadAsyncTest()
{
  std::unique_ptr<eos::fst::test::TestEnv> mEnv(new eos::fst::test::TestEnv());
  std::string address = "root://root@" + mEnv->GetMapping("server");
  std::string file_path = mEnv->GetMapping("plain_file");
  XrdCl::URL url(address);
  assert(url.IsValid());
  std::string file_url = address + "/" + file_path;
  std::unique_ptr<eos::fst::XrdIo> file(new eos::fst::XrdIo(file_url));
  std::cerr << "File url: " << file_url << std::endl;
  assert(!(file->fileOpen(SFS_O_RDONLY)));
  // Get file size
  struct stat buff = {0};
  assert(!(file->fileStat(&buff)));
  uint64_t file_size = buff.st_size;
  off_t buff_size = 1025 * 4;
  char buffer [buff_size];
  uint64_t offset = 0;

  while (offset <= file_size) {
    assert(buff_size == file->fileReadAsync(offset, buffer, buff_size,
                                            false));
    offset += buff_size;
  }

  assert(file->fileClose());
  (void) buffer; // make compiler happy
}

EOSFSTTEST_NAMESPACE_END
