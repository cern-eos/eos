// ----------------------------------------------------------------------
// File: Access.hh
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

/*----------------------------------------------------------------------------*/
/**
 * @file   Access.hh
 * 
 * @brief  Class defining access rules like banned users, hosts, stall rules etc.
 *       
 */
/*----------------------------------------------------------------------------*/


#ifndef __EOSCOMMON_ACCESS__
#define __EOSCOMMON_ACCESS__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
#include <map>
#include <vector>
#include <string>
#include <set>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/** @brief class implementing global access rules
 *
 * The access regulations are applied in XrdMgmOfs::ShouldStall & 
 * XrdMgmOfs::ShouldRedirect functions.\n
 * Normally User,Group & Host rules act as black-list and the AllowedXX rules 
 * exclude individuals from the black list.\n\n
 * The stall rules can be:\n
 * '*' => everything get's stalled by number of seconds stored 
 * in gStallRules["*"]\n
 * 'r:*" => everything get's stalled in read operations as above.\n
 *'w:*" => everything get's stalled in write operations as above.\n\n
 * The same syntax is used in gRedirectionRules to define r+w, 
 * r or w operation redirection. 
 * The value in this map is defined as '<host>:<port>'
 */
/*----------------------------------------------------------------------------*/
class Access
{
private:
public:

  // static key defining the ban users entry in the global configuration 
  // key-value map
  static const char* gUserKey;

  // static key defining the ban group key in the global configuration 
  // key-value map
  static const char* gGroupKey; 

  // static key defining the ban host key in the global configuration 
  // key-value map
  static const char* gHostKey;

  // static key defining the allowed users key in the global configuration 
  // key-value map
  static const char* gAllowedUserKey;

  // static key defining the allowed group key in the global configuration 
  // key-value map
  static const char* gAllowedGroupKey;

  // static key defining the allowed host key in the global configuration
  // key-value map
  static const char* gAllowedHostKey;

  // static key defining the stall rules in the global configuration 
  // key-value map
  static const char* gStallKey;

  // static key defining the redirection rules in the global configuration 
  // key-value map
  static const char* gRedirectionKey;

  // set containing the banned user ID
  static std::set<uid_t> gBannedUsers;

  //! set containing the banned group ID
  static std::set<gid_t> gBannedGroups;

  //! set containing the allowed user IDs
  static std::set<uid_t> gAllowedUsers;

  //! set containing the allowed group IDs
  static std::set<gid_t> gAllowedGroups;

  //! set containing the banned host names
  static std::set<std::string> gBannedHosts;

  //! set containing the allowed host names
  static std::set<std::string> gAllowedHosts;

  //! map containing redirection rules
  static std::map<std::string, std::string> gRedirectionRules;

  //! map containing stall rules
  static std::map<std::string, std::string> gStallRules;

  //! map containint stall message comment
  static std::map<std::string, std::string> gStallComment;

  //! indicates global stall rule
  static bool gStallGlobal;

  //! indicates global read stall
  static bool gStallRead;

  //! indicates global write stall
  static bool gStallWrite;

  //! indicates a user or group rate stall entry
  static bool gStallUserGroup;

  //! map containing user based redirection
  static std::map<uid_t, std::string> gUserRedirection;

  //! map containing group based redirection
  static std::map<gid_t, std::string> gGroupRedirection;

  //! global rw mutex protecting all static set's and maps in Access
  static eos::common::RWMutex gAccessMutex;

  // ---------------------------------------------------------------------------
  // reset/cleear all access rules
  // ---------------------------------------------------------------------------
  static void Reset ();

  // ---------------------------------------------------------------------------
  // retrieve the access configuration from the global shared 
  // hash/config engine and fill all static access configuration variables
  // ---------------------------------------------------------------------------
  static void ApplyAccessConfig ();

  // ---------------------------------------------------------------------------
  // store the global access configuration variable into the global
  // shared hash/config engine
  // ---------------------------------------------------------------------------
  static bool StoreAccessConfig ();
};

EOSMGMNAMESPACE_END

#endif
