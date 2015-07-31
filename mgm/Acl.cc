// ----------------------------------------------------------------------
// File: Acl.cc
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
#include "mgm/Acl.hh"
#include "mgm/Egroup.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include <regex.h>
#include <string>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//!
//! Constructor
//!
//! @param sysacl system acl definition string
//! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)(+u)}|z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
//! @param useracl user acl definition string
//! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)(+u)}|z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
//! @param vid virtual id to match ACL
//! @param allowUserAcl if true evaluate also the user acl for the permissions
//!
//------------------------------------------------------------------------------
Acl::Acl (std::string sysacl,
          std::string useracl,
          eos::common::Mapping::VirtualIdentity &vid,
          bool allowUserAcl)
{
  Set(sysacl, useracl, vid, allowUserAcl);
}

//------------------------------------------------------------------------------
//!
//! Constructor
//!
//! @param parent path where to read the acl attributes from
//! @param error return error object
//! @param vid virtual id to match ACL
//! @param attr map returns all the attributes from path
//! @param lockNs should we lock the namespace when retrieveng the attribute map
//!
//------------------------------------------------------------------------------

Acl::Acl (const char* path,
          XrdOucErrInfo &error,
          eos::common::Mapping::VirtualIdentity &vid,
          eos::IContainerMD::XAttrMap &attrmap,
          bool lockNs)
{
  // get attributes
  gOFS->_attr_ls(path, error, vid, 0, attrmap, lockNs);
  // define the acl rules from the attributes
  Set(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
      attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""),
      vid, attrmap.count("sys.eval.useracl"));
}


//------------------------------------------------------------------------------
//!
//! @brief Set the contents of an ACL and compute the canXX and hasXX booleans.
//!
//! @param sysacl system acl definition string
//! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{arwxom(!d)(+d)(!u)}|z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
//! @param useracl user acl definition string
//! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)}|z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
//! @param vid virtual id to match ACL
//! @param allowUserAcl if true evaluate the user acl for permissions
//!
//------------------------------------------------------------------------------

void
Acl::Set (std::string sysacl,
          std::string useracl,
          eos::common::Mapping::VirtualIdentity &vid,
          bool allowUserAcl)

{
  std::string acl = "";
  if (sysacl.length())
  {
    acl += sysacl;
  }
  if (allowUserAcl)
  {
    if (useracl.length())
    {
      if (sysacl.length())
        acl += ",";
      acl += useracl;
    }
  }

  // ---------------------------------------------------------------------------
  // by default nothing is granted
  // ---------------------------------------------------------------------------
  hasAcl = false;
  canRead = false;
  canWrite = false;
  canWriteOnce = false;
  canUpdate = true;
  canBrowse = false;
  canChmod = false;
  canNotChmod = false;
  canChown = false;
  canNotDelete = false;
  canDelete = false;
  canSetQuota = false;
  hasEgroup = false;
  isMutable = true;
  canArchive = false;

  // no acl definition
  if (!acl.length())
    return;


  int errc = 0;
  std::vector<std::string> rules;
  std::string delimiter = ",";
  eos::common::StringConversion::Tokenize(acl, rules, delimiter);
  std::vector<std::string>::const_iterator it;

  XrdOucString sizestring1;
  XrdOucString sizestring2;

  for (size_t n_gid = 0; n_gid < vid.gid_list.size(); ++n_gid)
  {
    gid_t chk_gid = vid.gid_list[n_gid];
    // only check non system groups
    if (chk_gid < 3)
      continue;
    std::string userid = eos::common::StringConversion::GetSizeString(sizestring1, (unsigned long long) vid.uid);
    std::string groupid = eos::common::StringConversion::GetSizeString(sizestring2, (unsigned long long) chk_gid);

    std::string usertag = "u:";
    usertag += userid;
    usertag += ":";
    std::string grouptag = "g:";
    grouptag += groupid;
    grouptag += ":";

    std::string username = eos::common::Mapping::UidToUserName(vid.uid, errc);

    if (errc)
      username = "_INVAL_";

    std::string groupname = eos::common::Mapping::GidToGroupName(chk_gid, errc);

    if (errc)
      groupname = "_INVAL_";

    std::string usertagfn = "u:";
    usertagfn += username;
    usertagfn += ":";
    std::string grouptagfn = "g:";
    grouptagfn += groupname;
    grouptagfn += ":";
    std::string ztag = "z:";

    eos_static_debug("%s %s %s %s", usertag.c_str(), grouptag.c_str(), usertagfn.c_str(), grouptagfn.c_str());
    // ---------------------------------------------------------------------------
    // Rule interpretation logic
    // ---------------------------------------------------------------------------
    for (it = rules.begin(); it != rules.end(); it++)
    {
      bool egroupmatch = false;
      // -------------------------------------------------------------------------
      // check for e-group membership
      // -------------------------------------------------------------------------
      if (!it->compare(0, strlen("egroup:"), "egroup:"))
      {
        std::vector<std::string> entry;
        std::string delimiter = ":";
        eos::common::StringConversion::Tokenize(*it, entry, delimiter);

        if (entry.size() < 3)
          continue;

        egroupmatch = Egroup::Member(username, entry[1]);
        hasEgroup = egroupmatch;
      }

      // ---------------------------------------------------------------------------
      // match 'our' rule
      // ---------------------------------------------------------------------------
      if ((!it->compare(0, usertag.length(), usertag)) ||
          (!it->compare(0, grouptag.length(), grouptag)) ||
          (!it->compare(0, ztag.length(), ztag)) ||
          (egroupmatch) ||
          (!it->compare(0, usertagfn.length(), usertagfn))
          || (!it->compare(0, grouptagfn.length(), grouptagfn)))
      {
        // that is our rule
        std::vector<std::string> entry;
        std::string delimiter = ":";
        eos::common::StringConversion::Tokenize(*it, entry, delimiter);

        if (entry.size() < 3)
        {
          // z tag entries have only two fields
          if (it->compare(0, ztag.length(), ztag) || (entry.size() < 2))
            continue;
          // add an empty entry field
          entry.resize(3);
          entry[2] = entry[1];
        }

        // 'a' defines archiving permission
        if ((entry[2].find("a")) != std::string::npos)
        {
          canArchive = true;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // 'r' defines read permission
        // -----------------------------------------------------------------------
        if ((entry[2].find("r")) != std::string::npos)
        {
          canRead = true;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // 'x' defines browsing permission
        // -----------------------------------------------------------------------
        if ((entry[2].find("x")) != std::string::npos)
        {
          canBrowse = true;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // 'm' defines mode change permission
        // -----------------------------------------------------------------------
        if ((entry[2].find("m")) != std::string::npos)
        {
          if ((entry[2].find("!m")) != std::string::npos)
          {
            canNotChmod = true;
          }
          else
          {
            canChmod = true;
          }
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // 'c' defines owner change permission (for directories)
        // -----------------------------------------------------------------------
        if ((!useracl.length()) && ((entry[2].find("c")) != std::string::npos))
        {
          // this is only valid if only a sysacl is present
          canChown = true;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // '!d' forbids deletion
        // -----------------------------------------------------------------------
        if ((entry[2].find("!d")) != std::string::npos)
        {
          // canDelete is true, if deletion has been explicitly allowed by a rule
          // and in this case we don't forbid deletion even if another rule
          // says that
          if (!canDelete)
          {
            canNotDelete = true;
          }

          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // '+d' adds deletion
        // -----------------------------------------------------------------------
        if ((entry[2].find("+d")) != std::string::npos)
        {
          canDelete = true;
          canNotDelete = false;
          canWriteOnce = false;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // '!u' removes update
        // -----------------------------------------------------------------------
        if ((entry[2].find("!u")) != std::string::npos)
        {
          canUpdate = false;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // '+u' adds update
        // -----------------------------------------------------------------------
        if ((entry[2].find("+u")) != std::string::npos)
        {
          canUpdate = true;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // 'wo' defines write once permissions
        // -----------------------------------------------------------------------
        if (((entry[2].find("wo")) != std::string::npos))
        {
          canWriteOnce = true;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // 'w' defines write permissions if 'wo' is not granted
        // -----------------------------------------------------------------------
        if ((!canWriteOnce) && (entry[2].find("w")) != std::string::npos)
        {
          canWrite = true;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // 'q' defines quota set permission
        // -----------------------------------------------------------------------
        if ((!useracl.length()) && ((entry[2].find("q")) != std::string::npos))
        {
          // this is only valid if only a sys acl is present
          canSetQuota = true;
          hasAcl = true;
        }

        // -----------------------------------------------------------------------
        // 'i' makes directories immutable
        // -----------------------------------------------------------------------
        if ((entry[2].find("i")) != std::string::npos)
        {
          isMutable = false;
          hasAcl = true;
        }
      }
    }
  }
}


//------------------------------------------------------------------------------
//!
//! @brief Check whether ACL has a valid format / syntax.
//!
//! @param value value to check
//! @param error error datastructure
//! @param sysacl boolean indicating a sys acl entry which might have a z: rule
//!
//! return boolean indicating validity
//------------------------------------------------------------------------------

bool
Acl::IsValid (const std::string value,
              XrdOucErrInfo &error,
              bool sysacl)
{
  // empty is valid
  if (!value.length())
    return true;

  int regexErrorCode;
  int result;
  regex_t regex;
  std::string regexString = "^(((((u|g):(([0-9]+)|([\\.[:alnum:]-]+)))|"
          "(egroup:([\\.[:alnum:]-]+))):"
          "(a|r|w|wo|x|i|m|!m|!d|[+]d|!u|[+]u|q|c)+)[,]?)*$";

  if (sysacl)
  {
    regexString = "^(((((u|g):(([0-9]+)|([\\.[:alnum:]-]+)))|"
            "(egroup:([\\.[:alnum:]-]+))|"
            "(z)):(a|r|w|wo|x|i|m|!m|!d|[+]d|!u|[+]u|q|c)+)[,]?)*$";
  }

  // Compile regex
  regexErrorCode = regcomp(&regex, regexString.c_str(), REG_EXTENDED);

  if (regexErrorCode)
  {
    error.setErrInfo(2, "failed to compile regex");
    regfree(&regex);
    return false;
  }

  // Execute regex
  result = regexec(&regex, value.c_str(), 0, NULL, 0);
  regfree(&regex);

  // Check the result
  if (result == 0)
  {
    return true;
  }
  else if (result == REG_NOMATCH)
  {
    error.setErrInfo(1, "invalid acl syntax");
    return false;
  }
  else // REG_BADPAT, REG_ESPACE, etc...
  {
    error.setErrInfo(2, "invalid regex or out of memory");
    return false;
  }
}

EOSMGMNAMESPACE_END
