//------------------------------------------------------------------------------
// File: AuthFsTest.cc
// Author: Elvin Sindrilaru  <esindril@cern.ch> CERN
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

#include <gtest/gtest.h>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClFile.hh>
#include <XrdOuc/XrdOucErrInfo.hh>
#include <XrdSec/XrdSecEntity.hh>
#include "TestEnv.hh"

//------------------------------------------------------------------------------
//! AuthFsTest class
//------------------------------------------------------------------------------
class AuthFsTest_F: public ::testing::Test
{
  protected::
  //----------------------------------------------------------------------------
  //! setUp function called before each test is done
  //----------------------------------------------------------------------------
  void SetUp(void) override
  {
    // Initialise
    mEnv = new eos::auth::test::TestEnv();
    std::string address = "root://root@" + mEnv->GetMapping("server");
    XrdCl::URL url(address);
    mFs = new XrdCl::FileSystem(url);
  }

  //----------------------------------------------------------------------------
  //! tearDown function after each test is done
  //----------------------------------------------------------------------------
  void TearDown(void) override
  {
    delete mEnv;
    delete mFs;
    mFs = 0;
  }

private:
  XrdCl::FileSystem* mFs; ///< XrdCl::FileSystem instance used in the tests
  eos::auth::test::TestEnv* mEnv; ///< testing envirionment object
};


//------------------------------------------------------------------------------
// File stat test - for a file which exists in EOS
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, StatTest)
{
  XrdCl::StatInfo* stat {nullptr};
  uint64_t file_size = atoi(mEnv->GetMapping("file_size").c_str());
  std::string file_path = mEnv->GetMapping("file_path");
  XrdCl::XRootDStatus status = mFs->Stat(file_path, stat);
  ASSERT_TRUE(status.IsOK());
  ASSERT_TRUE(stat != nullptr);
  ASSERT_TRUE(stat->GetSize() == file_size); // 1MB
  ASSERT_TRUE(stat->TestFlags(XrdCl::StatInfo::IsReadable)) ;
  delete stat;
  stat = 0;
}

//------------------------------------------------------------------------------
// File stat test - for a file which does not exits in EOS
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, : StatFailTest)
{
  XrdCl::StatInfo* stat {nullptr};
  std::string file_path = mEnv->GetMapping("file_missing");
  XrdCl::XRootDStatus status = mFs->Stat(file_path, stat);
  ASSERT_FALSE(status.IsOK());
  ASSERT_TRUE(stat == 0);
  delete stat;
  stat = 0;
}

//------------------------------------------------------------------------------
// StatVFS test - this request goes to the XrdMgmOfs::fsctl with command id
// SFS_FSCTL_STATFS = 2 and is not supported by EOS i.e. returns an error
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, StatVFSTest)
{
  XrdCl::StatInfoVFS* statvfs {nullptr};
  XrdCl::XRootDStatus status = mFs->StatVFS("/", statvfs);
  ASSERT_TRUE(status.IsError());
  ASSERT_TRUE(status.code == XrdCl::errErrorResponse);
  delete statvfs;
}

//------------------------------------------------------------------------------
// Truncate test
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, TruncateTest)
{
  std::string file_path = mEnv->GetMapping("file_path");
  XrdCl::XRootDStatus status = mFs->Truncate(file_path, 1024);
  ASSERT_TRUE(status.IsError());
  ASSERT_TRUE(status.code == XrdCl::errErrorResponse);
}

//------------------------------------------------------------------------------
// Rename test
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, RenameTest)
{
  uint64_t file_size = atoi(mEnv->GetMapping("file_size").c_str());
  std::string file_path = mEnv->GetMapping("file_path");
  std::string rename_path = mEnv->GetMapping("file_rename");
  XrdCl::XRootDStatus status = mFs->Mv(file_path, rename_path);
  ASSERT_TRUE(status.IsOK());
  // Stat the renamed file
  XrdCl::StatInfo* stat {nullptr};
  status = mFs->Stat(rename_path, stat);
  ASSERT_TRUE(status.IsOK());
  ASSERT_TRUE(stat != nullptr);
  ASSERT_TRUE(stat->GetSize() == file_size);
  ASSERT_TRUE(stat->TestFlags(XrdCl::StatInfo::IsReadable)) ;
  delete stat;
  stat = 0;
  // Rename back to initial file name
  status = mFs->Mv(rename_path, file_path);
  ASSERT_TRUE(status.IsOK());
  // Stat again the initial file name
  status = mFs->Stat(file_path, stat);
  ASSERT_TRUE(status.IsOK());
  ASSERT_TRUE(stat != nullptr);
  ASSERT_TRUE(stat->GetSize() == file_size);
  ASSERT_TRUE(stat->TestFlags(XrdCl::StatInfo::IsReadable)) ;
  delete stat;
  stat = 0;
}

//------------------------------------------------------------------------------
// Rem test. This also tests the normal writing mode in EOS i.e. the redirection
// to the FST node.
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, RemTest)
{
  using namespace XrdCl;
  // Create a dummy file
  std::string address = "root://root@" + mEnv->GetMapping("server") + "/";
  XrdCl::URL url(address);
  ASSERT_TRUE(url.IsValid());
  // Construct the file path
  std::string file_path = mEnv->GetMapping("dir_name") + "/to_delete.dat";
  std::string file_url = address + file_path;
  // Fill 1MB buffer with random content
  int buff_size = atoi(mEnv->GetMapping("file_size").c_str());
  char* buffer = new char[buff_size];
  std::fstream urand("/dev/urandom");
  urand.read(buffer, buff_size);
  urand.close();
  // Create and write a 1MB file
  File file;
  ASSERT_TRUE(file.Open(file_url, OpenFlags::Delete | OpenFlags::Update,
                        Access::UR | Access::UW | Access::GR | Access::OR).IsOK());
  ASSERT_TRUE(file.Write(0, buff_size, buffer).IsOK());
  ASSERT_TRUE(file.Sync().IsOK());
  ASSERT_TRUE(file.Close().IsOK());
  // Delete the newly created file
  ASSERT_TRUE(mFs->Rm(file_path).IsOK());
  delete[] buffer;
}

//------------------------------------------------------------------------------
// Prepare test - EOS does not support this, it just returns SFS_OK. It seems
// no one knows what it should do exactly ... :)
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, PrepareTest)
{
  using namespace XrdCl;
  std::string file_path = mEnv->GetMapping("file_path");
  std::vector<std::string> file_list;
  file_list.push_back(file_path);
  Buffer* response = 0;
  XRootDStatus status = mFs->Prepare(file_list, PrepareFlags::Flags::WriteMode,
                                     3, response);
  ASSERT_TRUE(status.IsOK());
  delete response;
}

//------------------------------------------------------------------------------
// Mkdir test
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, MkRemDirTest)
{
  using namespace XrdCl;
  std::string dir_path = mEnv->GetMapping("dir_new");
  MkDirFlags::Flags flags = MkDirFlags::Flags::MakePath;
  Access::Mode mode = Access::Mode::UR | Access::Mode::UW |
                      Access::Mode::GR | Access::Mode::OR;
  XRootDStatus status = mFs->MkDir(dir_path, flags, mode);
  ASSERT_TRUE(status.IsOK());
  // Delete the newly created directory
  status = mFs->RmDir(dir_path);
  ASSERT_TRUE(status.IsOK());
}

//------------------------------------------------------------------------------
// fsctl test - in XRootD this is called for the following: query for space,
// locate, stats or xattr. In practice in EOS we only support locate and stats
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, fsctlTest)
{
  using namespace XrdCl;
  Buffer* response = 0;
  Buffer arg;
  arg.FromString("/");
  // SFS_FSCTL_STATLS is supported
  XRootDStatus status = mFs->Query(QueryCode::Code::Space, arg, response);
  ASSERT_TRUE(status.IsOK());
  ASSERT_TRUE(response->GetSize() != 0);
  delete response;
  response = 0;
  // This calls getStats() on the EosAutOfs
  status = mFs->Query(QueryCode::Code::Stats, arg, response);
  ASSERT_TRUE(status.IsOK());
  delete response;
  response = 0;
  // This is not supported - should return an error
  status = mFs->Query(QueryCode::Code::XAttr, arg, response);
  ASSERT_TRUE(status.IsError());
  delete response;
  response = 0;
  // Test xattr query which calls fsctl with cmd = SFS_FSCTL_STATXS on the
  // server side which is not supported in EOS
  status = mFs->Query(QueryCode::Code::XAttr, arg, response);
  ASSERT_TRUE(status.IsError());
  delete response;
  response = 0;
  // Test Locate which calls fsctl with cmd = SFS_FSCTL_LOCATE on the server size
  std::string file_path = mEnv->GetMapping("file_path");
  LocationInfo* location;
  status = mFs->Locate(file_path, OpenFlags::Flags::Read, location);
  ASSERT_TRUE(status.IsOK());
  ASSERT_TRUE(location);
  delete location;
}

//------------------------------------------------------------------------------
// FSctl test - this is called only when we do an opaque query
//
// Note: QueryCode::Code::Opaque     -> SFS_FSCTL_PLUGIO
//       QueryCode::Code::OpaqueFile -> SFS_FSCTL_PLUGIN
//
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, FSctlTest)
{
  using namespace XrdCl;
  Buffer* response = 0;
  Buffer arg;
  // SFS_FSCTL_PLUGIN not supported - we expect an error
  XRootDStatus status = mFs->Query(QueryCode::Code::Opaque, arg, response);
  ASSERT_TRUE(status.IsError());
  arg.Release();
  delete response;
  response = 0;
  // Do stat on a file - which is an SFS_FSCTL_PLUGIO and is supported
  std::ostringstream sstr;
  sstr << "/?mgm.pcmd=stat&mgm.path=" << mEnv->GetMapping("file_path");
  arg.FromString(sstr.str());
  status = mFs->Query(QueryCode::Code::OpaqueFile, arg, response);
  ASSERT_TRUE(status.IsOK());
  ASSERT_TRUE(response->GetSize());
  delete response;
}

//------------------------------------------------------------------------------
// Chksum test
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, ChksumTest)
{
  using namespace XrdCl;
  std::string file_chksum = mEnv->GetMapping("file_chksum");
  Buffer* response = 0;
  Buffer arg;
  arg.FromString(mEnv->GetMapping("file_path"));
  XRootDStatus status = mFs->Query(QueryCode::Code::Checksum, arg, response);
  ASSERT_TRUE(status.IsOK());
  ASSERT_TRUE(response != nullptr);
  ASSERT_TRUE(response->GetSize() != 0);
  ASSERT_TRUE(response->ToString() == file_chksum);
  delete response;
}

//------------------------------------------------------------------------------
// Chmod test - only works on directories in EOS
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, ChmodTest)
{
  using namespace XrdCl;
  std::string dir_path = mEnv->GetMapping("dir_new");
  std::string file_path = mEnv->GetMapping("file_path");
  //Create dummy directory
  MkDirFlags::Flags flags = MkDirFlags::Flags::MakePath;
  Access::Mode mode = Access::Mode::UR | Access::Mode::UW |
                      Access::Mode::GR | Access::Mode::OR;
  XRootDStatus status = mFs->MkDir(dir_path, flags, mode);
  ASSERT_TRUE(status.IsOK());
  // Chmod dir
  status = mFs->ChMod(dir_path,
                      Access::Mode::UR | Access::Mode::UW | Access::Mode::UX |
                      Access::Mode::GR | Access::Mode::GW | Access::Mode::GX |
                      Access::Mode::OR | Access::Mode::OW | Access::Mode::OX);
  ASSERT_TRUE(status.IsOK());
  // Delete the newly created directory
  status = mFs->RmDir(dir_path);
  ASSERT_TRUE(status.IsOK());
  // Chmod file
  status = mFs->ChMod(file_path,
                      Access::Mode::UR | Access::Mode::UW | Access::Mode::UX |
                      Access::Mode::GR | Access::Mode::GW | Access::Mode::GX |
                      Access::Mode::OR | Access::Mode::OW | Access::Mode::OX);
  ASSERT_TRUE(status.IsOK());
}

//------------------------------------------------------------------------------
// Directory listing test - the initial directory should contain only the
// initial test file: file1MB.dat
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, DirListTest)
{
  using namespace XrdCl;
  std::string dir_path = mEnv->GetMapping("dir_name");
  DirectoryList* list_dirs = 0;
  XRootDStatus status = mFs->DirList(dir_path, DirListFlags::None, list_dirs);
  ASSERT_TRUE(status.IsOK());
  ASSERT_TRUE(list_dirs->GetSize() == 1);
  ASSERT_TRUE(list_dirs->GetParentName() == dir_path);
  delete list_dirs;
}

//------------------------------------------------------------------------------
// Proc command test which actually tests the File implementation. Try to do an
// "fs ls" command as an administrator.
//------------------------------------------------------------------------------
TEST_F(AuthFsTest_F, ProcCommandTest)
{
  using namespace XrdCl;
  std::string address = "root://root@" + mEnv->GetMapping("server") + "/";
  XrdCl::URL url(address);
  ASSERT_TRUE(url.IsValid());
  // Construct the file path
  std::string command = "mgm.cmd=fs&mgm.subcmd=ls&eos.ruid=0&eos.rgid=0";
  std::string file_path = "/proc/admin/?" + command;
  std::string file_url = address + file_path + command;
  // Open the file for reading - which triggers the command to be executed and
  // then we just need to read the result of the command from the same file
  File file;
  ASSERT_TRUE(file.Open(file_url, OpenFlags::Read).IsOK());
  // Prepare buffer which contains the result
  std::string output("");
  uint64_t offset = 0;
  uint32_t nread = 0;
  char buffer[4096 + 1];
  XRootDStatus status = file.Read(offset, 4096, buffer, nread);

  while (status.IsOK() && (nread > 0)) {
    buffer[nread] = '\0';
    output += buffer;
    offset += nread;
    status = file.Read(offset, 4096, buffer, nread);
  }

  ASSERT_TRUE(output.length() != 0);
  ASSERT_TRUE(file.Close().IsOK());
}
