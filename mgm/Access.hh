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

/**
 * @file   Access.hh
 * 
 * @brief  Class defining access rules like banned users, hosts, stall rules etc.
 *       
 * The access regulations are applied in XrdMgmOfs::ShouldStall & XrdMgmOfs::ShouldRedirect functions.
 * Normally User,Group & Host rules act as black-list and the AllowedXX rules exclude individuals from the black list.
 * The stall rules can be:
 * '*' => everything get's stalled by number of seconds stored in gStallRules["*"]
 * 'r:*" => everything get's stalled in read operations as above.
 * 'w:*" => everything get's stalled in write operations as above.
 * The same syntax is used in gRedirectionRules to define r+w, r or w operation redirection. The value in this map is defined as '<host>:<port>'
 */


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
//! Class implementing global Acccess rules
/*----------------------------------------------------------------------------*/
class Access
{
private:
public:


  static const char* gUserKey; //< static key defining the ban users entry in the global configuration key-value map
  static const char* gGroupKey; //< static key defining the ban group key in the global configuration key-value map
  static const char* gHostKey; //< static key defining the ban host key in the global configuration key-value map
  static const char* gAllowedUserKey; //< static key defining the allowed users key in the global configuration key-value map
  static const char* gAllowedGroupKey; //< static key defining the allowed group key in the global configuration key-value map
  static const char* gAllowedHostKey; //< static key defining the allowed host key in the global configuration key-value map
  static const char* gStallKey; //< static key defining the stall rules in the global configuration key-value map
  static const char* gRedirectionKey; //< static key defining the redirection rules in the global configuration key-value map
  static std::set<uid_t> gBannedUsers; //< set containing the banned user IDs
  static std::set<gid_t> gBannedGroups; //< set containing the banned group IDs
  static std::set<uid_t> gAllowedUsers; //< set containing the allowed user IDs
  static std::set<gid_t> gAllowedGroups; //< set containing the allowed group IDs
  static std::set<std::string> gBannedHosts; //< set containing the banned host names
  static std::set<std::string> gAllowedHosts; //< set containing the allowed host names

  static std::map<std::string, std::string> gRedirectionRules; //< map containing redirection rules
  static std::map<std::string, std::string> gStallRules; //< map containing stall rules
  static std::map<std::string, std::string> gStallComment; //< map containint stall message comment
  static bool gStallGlobal; //< indicates global stall rule
  static bool gStallRead; //< indicates global read stall
  static bool gStallWrite; //< indicates global write stall
  static bool gStallUserGroup; //< indicates a user or group rate stall entry
  static std::map<uid_t, std::string> gUserRedirection; //< map containing user based redirection
  static std::map<gid_t, std::string> gGroupRedirection; //< map containing group based redirection

  static eos::common::RWMutex gAccessMutex; //< globa rw mutex protecting all static set's and maps in Access

  // ---------------------------------------------------------------------------
  //! Reset/cleear all access rules
  // ---------------------------------------------------------------------------
  static void Reset ();

  // ---------------------------------------------------------------------------
  //! Retrieve the access configuration from the global shared hash/config engine and fill all static access configuration variables
  // ---------------------------------------------------------------------------
  static void ApplyAccessConfig ();

  // ---------------------------------------------------------------------------
  //! Store the global access configuration variable into the globa shared hash/config engine
  // ---------------------------------------------------------------------------
  static bool StoreAccessConfig ();
};

EOSMGMNAMESPACE_END

#endif
