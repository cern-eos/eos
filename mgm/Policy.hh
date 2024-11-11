// ----------------------------------------------------------------------
// File: Policy.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_POLICY__HH__
#define __EOSMGM_POLICY__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/Scheduler.hh"
#include "common/Mapping.hh"
#include "namespace/interface/IContainerMD.hh"
/*----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucEnv.hh>
/*----------------------------------------------------------------------------*/
#include <sys/types.h>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Policy
{
public:

  Policy() { };

  ~Policy() { };

  static void GetLayoutAndSpace(const char* path,
                                eos::IContainerMD::XAttrMap& map,
                                const eos::common::VirtualIdentity& vid,
                                unsigned long& layoutId,
                                std::string& space,
                                XrdOucEnv& env,
                                unsigned long& forcedfsid,
                                long& forcedgroup,
                                std::string& bandwidth,
                                bool& schedul,
                                std::string& iopriority,
                                std::string& ioptype,
                                bool isrw,
                                bool lock_view = false,
                                uint64_t* atimeage = 0);

  static void GetPlctPolicy(const char* path,
                            eos::IContainerMD::XAttrMap& map,
                            const eos::common::VirtualIdentity& vid,
                            XrdOucEnv& env,
                            eos::mgm::Scheduler::tPlctPolicy& plctpo,
                            std::string& targetgeotag);

  static bool RedirectLocal(const char* path,
                            eos::IContainerMD::XAttrMap& map,
                            const eos::common::VirtualIdentity& vid,
                            unsigned long& layoutId,
                            const std::string& space,
                            XrdOucEnv& env
                           );

  static unsigned long GetSpacePolicyLayout(const char* space);


  static bool Set(const char* value);
  static bool Set(XrdOucEnv& env, int& retc, XrdOucString& stdOut,
                  XrdOucString& stdErr);
  static void Ls(XrdOucEnv& env, int& retc, XrdOucString& stdOut,
                 XrdOucString& stdErr);
  static bool Rm(XrdOucEnv& env, int& retc, XrdOucString& stdOut,
                 XrdOucString& stdErr);


  static bool IsProcConversion(const char* path);

  static const char* Get(const char* key);

  struct RWParams;

  static inline std::vector<std::string> GetConfigKeys()
  {
    return gBasePolicyKeys;
  }

  static std::vector<std::string> GetRWConfigKeys(const RWParams& params);

  static void GetRWValue(const std::map<std::string, std::string>& conf_map,
                         const std::string& key_name,
                         const RWParams& params,
                         std::string& value);

  static const std::vector<std::string> gBasePolicyKeys;
  static const std::vector<std::string> gBaseLocalPolicyKeys;
  static const std::vector<std::string> gBasePolicyRWKeys;

  static double GetDefaultSizeFactor(std::shared_ptr<eos::IContainerMD> cmd);

  struct RWParams {
    std::string user_key;
    std::string group_key;
    std::string app_key;
    std::string rw_marker;

    RWParams(const std::string& user_str,
             const std::string& group_str,
             const std::string& app_str,
             bool is_rw) :
      user_key(".user:" + user_str),
      group_key(".group:" + group_str),
      app_key(".app:" + app_str),
      rw_marker(is_rw ? ":w" : ":r")
    {}

    std::string getKey(const std::string& key) const
    {
      return key + rw_marker;
    }

    std::vector<std::string> getKeys(const std::string& key) const;
  };
};

EOSMGMNAMESPACE_END

#endif
