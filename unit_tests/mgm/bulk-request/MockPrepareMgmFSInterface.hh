//------------------------------------------------------------------------------
//! @file MockPrepareMgmFSInterface.hh
//! @author Cedric Caffy - CERN
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

#ifndef EOS_MOCKPREPAREMGMFSINTERFACE_HH
#define EOS_MOCKPREPAREMGMFSINTERFACE_HH

#include "mgm/Namespace.hh"
#include <XrdOuc/XrdOucErrInfo.hh>
#include <XrdSfs/XrdSfsInterface.hh>
#include <XrdSec/XrdSecEntity.hh>
#include "common/VirtualIdentity.hh"
#include "namespace/interface/IContainerMD.hh"
#include "mgm/bulk-request/interface/IMgmFileSystemInterface.hh"
#include <sys/types.h>
#include <gmock/gmock.h>

EOSBULKNAMESPACE_BEGIN

class MockPrepareMgmFSInterface : public IMgmFileSystemInterface
{
public:
  MOCK_METHOD4(addStats, void(const char* tag, uid_t uid, gid_t gid,
                              unsigned long val));
  MOCK_METHOD0(isTapeEnabled, bool());
  MOCK_METHOD0(getReqIdMaxCount, int());
  MOCK_METHOD5(Emsg, int(const char* pfx, XrdOucErrInfo& einfo, int ecode,
                         const char* op, const char* target));
  MOCK_METHOD5(_exists, int(const char* path, XrdSfsFileExistence& file_exists,
                            XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo));
  MOCK_METHOD6(_exists, int(const char* path, XrdSfsFileExistence& file_exists,
                            XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* opaque,
                            bool take_lock));
  MOCK_METHOD6(_attr_ls, int(const char* path, XrdOucErrInfo& out_error,
                             const eos::common::VirtualIdentity& vid, const char* opaque,
                             eos::IContainerMD::XAttrMap& map, bool links));
  MOCK_METHOD5(_access, int(const char* path, int mode, XrdOucErrInfo& error,
                            eos::common::VirtualIdentity& vid, const char* info));
  MOCK_METHOD4(FSctl, int(const int cmd, XrdSfsFSctl& args, XrdOucErrInfo& error,
                          const XrdSecEntity* client));
  MOCK_METHOD8(_stat, int(const char* Name, struct stat* buf,
                          XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque,
                          std::string* etag, bool follow, std::string* uri));
  MOCK_METHOD1(_stat_set_flags, void(struct stat* buf));
  MOCK_METHOD0(get_logId, std::string());
  MOCK_METHOD0(get_host, std::string());
  MOCK_METHOD1(writeEosReportRecord, void(const std::string& record));
  ~MockPrepareMgmFSInterface() {}

  /**
   * Lambdas that will be passed to the Invoke methods
   */
  //Lambda that will be called by the mock method _exists. This lambda will return that the file exists
  static std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo)>
  _EXISTS_FILE_EXISTS_LAMBDA;
  //Lambda that will be called by the mock method _exists. This lambda will return that the file does not exist
  static std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo)>
  _EXISTS_FILE_DOES_NOT_EXIST_LAMBDA;

  //Lambda that will be called by the mock method _exists. This lambda will return that the file exists
  static std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* opaque, bool take_lock)>
  _EXISTS_VID_FILE_EXISTS_LAMBDA;
  //Lambda that will be called by the mock method _exists. This lambda will return that the file does not exist
  static std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* opaque, bool take_lock)>
  _EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA;

  //Lambda that will be called by the mock method _attr_ls on the files' parent directory in the case of stage prepare
  static std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
  _ATTR_LS_STAGE_PREPARE_LAMBDA;
  //Lambda that will be called by the mock method _attr_ls on the files' parent directory in the case of abort prepare
  static std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
  _ATTR_LS_ABORT_PREPARE_LAMBDA;
  //Lambda that will be called by the mock method _attr_ls on the files' parent directory in the case of evict prepare
  static std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
  _ATTR_LS_EVICT_PREPARE_LAMBDA;
  //Lambda that will be called by the mock method _attr_ls on the files and will return empty RETRIEVE and empty ARCHIVE errors
  static std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
  _ATTR_LS_QUERY_PREPARE_NO_ERROR_LAMBDA;
  //Lambda that will be called by the mock method _attr_ls on the files and will return a RETRIEVE ERROR
  static std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
  _ATTR_LS_RETRIEVE_ERROR_LAMBDA;
  //Lambda that will be called by the mock method _attr_ls on the files and will return a ARCHIVE ERROR
  static std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
  _ATTR_LS_ARCHIVE_ERROR_LAMBDA;
  //Lambda that will be called by the mock method _attr_ls on the files and will return a ARCHIVE AND RETRIEVE ERROR
  static std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
  _ATTR_LS_ARCHIVE_RETRIEVE_ERROR_LAMBDA;
  //Lambda that will be called by the mock method _stat on the file. This lambda will fill the stat buffer to fake the fact that the file is on tape only
  static std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
  _STAT_FILE_ON_TAPE_ONLY;
  //Lambda that will be called by the mock method _stat on the file. This lambda will fill the stat buffer to fake the fact that the file is on disk only
  static std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
  _STAT_FILE_ON_DISK_ONLY;
  //Lambda that will be called by the mock method _stat on the file. This lambda will fill the stat buffer to fake the fact that the file is on disk AND on tape
  static std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
  _STAT_FILE_ON_DISK_AND_TAPE;
  //Lambda that will be called by the mock method _stat on the file. This lambda will fake an error of the stat method
  static std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
  _STAT_ERROR;

  static std::function<int(const char* path, int mode, XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* info)>
  _ACCESS_FILE_PREPARE_PERMISSION_LAMBDA;
  static std::function<int(const char* path, int mode, XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* info)>
  _ACCESS_FILE_NO_PREPARE_PERMISSION_LAMBDA;

  static inline const std::string ERROR_RETRIEVE_STR = "ERROR_RETRIEVE";
  static inline const std::string ERROR_ARCHIVE_STR = "ERROR_ARCHIVE";
  static inline const std::string RETRIEVE_REQ_ID = "RETRIEVE_REQ_ID";
  static inline const std::string RETRIEVE_REQ_TIME = "RETRIEVE_REQ_TIME";
  static inline const std::string ERROR_STAT_STR = "ERROR_STAT";
  static inline const std::string EOS_REPORT_STR_FORMAT =
    "(([^&=]+)=([^&]*))(&(([^&=]+)=([^&]*)))*";
};

EOSBULKNAMESPACE_END

#endif // EOS_MOCKPREPAREMGMFSINTERFACE_HH
