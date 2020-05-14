// ----------------------------------------------------------------------
// File: CommitHelper.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_COMMITHELPER__HH__
#define __EOSMGM_COMMITHELPER__HH__

#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/Mapping.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include <string>
#include <map>

#include "XrdOuc/XrdOucEnv.hh"

EOSMGMNAMESPACE_BEGIN

class CommitHelper
{
public:

  static thread_local eos::common::LogId tlLogId;
  typedef std::map<std::string, std::string> cgi_t;
  typedef std::map<std::string, bool> option_t;
  typedef std::map<std::string, int> param_t;
  typedef std::map<std::string, eos::common::Path> path_t;

  static void hex2bin_checksum(std::string& checksum, char* binchecksum);
  static int check_filesystem(eos::common::VirtualIdentity& vid,
                              unsigned long fsid,
                              CommitHelper::cgi_t& cgi,
                              CommitHelper::option_t& option,
                              CommitHelper::param_t& params,
                              std::string& emsg);

  static void grab_cgi(XrdOucEnv& env, CommitHelper::cgi_t& cgi);

  static void log_info(eos::common::VirtualIdentity& vid,
                       const eos::common::LogId& thread_logid,
                       CommitHelper::cgi_t& cgi,
                       CommitHelper::option_t& option,
                       CommitHelper::param_t& params);

  static void set_options(CommitHelper::option_t& option,
                          CommitHelper::cgi_t& cgi);

  static void init_oc(XrdOucEnv& env,
                      CommitHelper::cgi_t& cgi,
                      CommitHelper::option_t& option,
                      CommitHelper::param_t& params);

  static bool is_reconstruction(CommitHelper::option_t& option);

  static bool check_commit_params(CommitHelper::cgi_t& cgi);

  static void remove_scheduler(unsigned long long fid);

  static bool validate_size(eos::common::VirtualIdentity& vid,
                            std::shared_ptr<eos::IFileMD> fmd,
                            unsigned long fsid,
                            unsigned long long size,
                            CommitHelper::option_t& option);

  static bool validate_checksum(eos::common::VirtualIdentity& vid,
                                std::shared_ptr<eos::IFileMD> fmd,
                                eos::Buffer& checksumbuffer,
                                unsigned long long fsid,
                                CommitHelper::option_t& option);

  static void log_verifychecksum(eos::common::VirtualIdentity& vid,
                                 std::shared_ptr<eos::IFileMD>fmd,
                                 eos::Buffer& checksumbuffer,
                                 unsigned long fsid,
                                 CommitHelper::cgi_t& cgi,
                                 CommitHelper::option_t& option);

  static bool handle_location(eos::common::VirtualIdentity& vid,
                              unsigned long cid,
                              std::shared_ptr<eos::IFileMD> fmd,
                              unsigned long fsid,
                              unsigned long long size,
                              CommitHelper::cgi_t& cgi,
                              CommitHelper::option_t& option);

  static void handle_occhunk(eos::common::VirtualIdentity& vid,
                             std::shared_ptr<eos::IFileMD>& fmd,
                             CommitHelper::option_t& option,
                             std::map<std::string, int>& params);


  static void handle_checksum(eos::common::VirtualIdentity& vid,
                              std::shared_ptr<eos::IFileMD>fmd,
                              CommitHelper::option_t& option,
                              eos::Buffer& checksumbuffer);

  static bool commit_fmd(eos::common::VirtualIdentity& vid,
                         unsigned long cid,
                         std::shared_ptr<eos::IFileMD>fmd,
                         unsigned long long replica_size,
                         CommitHelper::option_t& option,
                         std::string& errmsg);

  static unsigned long long get_version_fid(
    eos::common::VirtualIdentity& vid,
    unsigned long long fid,
    CommitHelper::path_t& paths,
    CommitHelper::option_t& option);

  static void handle_versioning(eos::common::VirtualIdentity& vid,
                                unsigned long fid,
                                CommitHelper::path_t& paths,
                                CommitHelper::option_t& option,
                                std::string& delete_path);



};

EOSMGMNAMESPACE_END

#endif
