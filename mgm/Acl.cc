//------------------------------------------------------------------------------
//! @file Acl.cc
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

#include "mgm/Acl.hh"
#include "mgm/Egroup.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/StringConversion.hh"
#include <regex.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Acl::Acl(std::string sysacl, std::string useracl,
         eos::common::Mapping::VirtualIdentity& vid, bool allowUserAcl)
{
  Set(sysacl, useracl, vid, allowUserAcl);
}



//------------------------------------------------------------------------------
//!
//! Constructor
//!
//! @param attr map containing all extended attributes
//! @param vid virtual id to match ACL
//!
//------------------------------------------------------------------------------

Acl::Acl (eos::IContainerMD::XAttrMap& attrmap,
     eos::common::Mapping::VirtualIdentity &vid)
{
  // define the acl rules from the attributes
  Set(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
      attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid, attrmap.count("sys.eval.useracl"));
}





//------------------------------------------------------------------------------
// Constructor by path
//------------------------------------------------------------------------------
Acl::Acl(const char* path, XrdOucErrInfo& error,
         eos::common::Mapping::VirtualIdentity& vid,
         eos::IContainerMD::XAttrMap& attrmap, bool lockNs)
{
  gOFS->_attr_ls(path, error, vid, 0, attrmap, lockNs);
  // Set the acl rules from the attributes
  Set(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
      attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""),
      vid, attrmap.count("sys.eval.useracl"));
}

//------------------------------------------------------------------------------
// Set the contents of an ACL and compute the canXX and hasXX booleans.
//------------------------------------------------------------------------------
void
Acl::Set(std::string sysacl, std::string useracl,
         eos::common::Mapping::VirtualIdentity& vid, bool allowUserAcl)
{
  std::string acl = "";

  if (sysacl.length()) {
    acl += sysacl;
  }

  if (allowUserAcl) {
    if (useracl.length()) {
      if (sysacl.length()) {
        acl += ",";
      }

      acl += useracl;
    }
  }

  // By default nothing is granted
  mHasAcl = false;
  mCanRead = false;
  mCanWrite = false;
  mCanWriteOnce = false;
  mCanUpdate = true;
  mCanBrowse = false;
  mCanChmod = false;
  mCanNotChmod = false;
  mCanChown = false;
  mCanNotDelete = false;
  mCanDelete = false;
  mCanSetQuota = false;
  mHasEgroup = false;
  mIsMutable = true;
  mCanArchive = false;

  // no acl definition
  if (!acl.length()) {
    return;
  }

  int errc = 0;
  std::vector<std::string> rules;
  std::string delimiter = ",";
  eos::common::StringConversion::Tokenize(acl, rules, delimiter);
  std::vector<std::string>::const_iterator it;
  XrdOucString sizestring1;
  XrdOucString sizestring2;

  for (size_t n_gid = 0; n_gid < vid.gid_list.size(); ++n_gid) {
    gid_t chk_gid = vid.gid_list[n_gid];

    // Only check non-system groups
    if (chk_gid < 3) {
      continue;
    }

    std::string userid = eos::common::StringConversion::GetSizeString(sizestring1,
                         (unsigned long long) vid.uid);
    std::string groupid = eos::common::StringConversion::GetSizeString(sizestring2,
                          (unsigned long long) chk_gid);
    std::string usertag = "u:";
    usertag += userid;
    usertag += ":";
    std::string grouptag = "g:";
    grouptag += groupid;
    grouptag += ":";
    std::string username = eos::common::Mapping::UidToUserName(vid.uid, errc);

    if (errc) {
      username = "_INVAL_";
    }

    std::string groupname = eos::common::Mapping::GidToGroupName(chk_gid, errc);

    if (errc) {
      groupname = "_INVAL_";
    }

    std::string usr_name_tag = "u:";
    usr_name_tag += username;
    usr_name_tag += ":";
    std::string grp_name_tag = "g:";
    grp_name_tag += groupname;
    grp_name_tag += ":";
    std::string ztag = "z:";
    eos_static_debug("%s %s %s %s", usertag.c_str(), grouptag.c_str(),
                     usr_name_tag.c_str(), grp_name_tag.c_str());

    // Rule interpretation logic
    for (it = rules.begin(); it != rules.end(); it++) {
      bool egroupmatch = false;

      // Check for e-group membership
      if (!it->compare(0, strlen("egroup:"), "egroup:")) {
        std::vector<std::string> entry;
        std::string delimiter = ":";
        eos::common::StringConversion::Tokenize(*it, entry, delimiter);

        if (entry.size() < 3) {
          continue;
        }

        egroupmatch = Egroup::Member(username, entry[1]);
        mHasEgroup = egroupmatch;
      }

      // Match 'our' rule
      if ((!it->compare(0, usertag.length(), usertag)) ||
          (!it->compare(0, grouptag.length(), grouptag)) ||
          (!it->compare(0, ztag.length(), ztag)) ||
          (egroupmatch) ||
          (!it->compare(0, usr_name_tag.length(), usr_name_tag)) ||
          (!it->compare(0, grp_name_tag.length(), grp_name_tag))) {
        std::vector<std::string> entry;
        std::string delimiter = ":";
        eos::common::StringConversion::Tokenize(*it, entry, delimiter);

        if (entry.size() < 3) {
          // z tag entries have only two fields
          if (it->compare(0, ztag.length(), ztag) || (entry.size() < 2)) {
            continue;
          }

          // add an empty entry field
          entry.resize(3);
          entry[2] = entry[1];
        }

        // 'a' defines archiving permission
        if ((entry[2].find("a")) != std::string::npos) {
          mCanArchive = true;
          mHasAcl = true;
        }

        // 'r' defines read permission
        if ((entry[2].find("r")) != std::string::npos) {
          mCanRead = true;
          mHasAcl = true;
        }

        // 'x' defines browsing permission
        if ((entry[2].find("x")) != std::string::npos) {
          mCanBrowse = true;
          mHasAcl = true;
        }

        // 'm' defines mode change permission
        if ((entry[2].find("m")) != std::string::npos) {
          if ((entry[2].find("!m")) != std::string::npos) {
            mCanNotChmod = true;
          } else {
            mCanChmod = true;
          }

          mHasAcl = true;
        }

        // 'c' defines owner change permission (for directories)
        if ((sysacl.find(*it) != std::string::npos) &&
            ((entry[2].find("c")) != std::string::npos)) {
          // this is only valid if it is specified as a sysacl
          mCanChown = true;
          mHasAcl = true;
        }

        // '!d' forbids deletion
        if ((entry[2].find("!d")) != std::string::npos) {
          // canDelete is true, if deletion has been explicitly allowed by a rule
          // and in this case we don't forbid deletion even if another rule
          // says that
          if (!mCanDelete) {
            mCanNotDelete = true;
          }

          mHasAcl = true;
        }

        // '+d' adds deletion
        if ((entry[2].find("+d")) != std::string::npos) {
          mCanDelete = true;
          mCanNotDelete = false;
          mCanWriteOnce = false;
          mHasAcl = true;
        }

        // '!u' removes update
        if ((entry[2].find("!u")) != std::string::npos) {
          mCanUpdate = false;
          mHasAcl = true;
        }

        // '+u' adds update
        if ((entry[2].find("+u")) != std::string::npos) {
          mCanUpdate = true;
          mHasAcl = true;
        }

        // 'wo' defines write once permissions
        if (((entry[2].find("wo")) != std::string::npos)) {
          mCanWriteOnce = true;
          mHasAcl = true;
        }

        // 'w' defines write permissions if 'wo' is not granted
        if ((!mCanWriteOnce) && (entry[2].find("w")) != std::string::npos) {
          mCanWrite = true;
          mHasAcl = true;
        }

        // 'q' defines quota set permission
        if (((sysacl.find(*it) != std::string::npos)) &&
            ((entry[2].find("q")) != std::string::npos)) {
          // this is only valid if it is specified as a sysacl
          mCanSetQuota = true;
          mHasAcl = true;
        }

        // 'i' makes directories immutable
        if ((entry[2].find("i")) != std::string::npos) {
          mIsMutable = false;
          mHasAcl = true;
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Check whether ACL has a valid format / syntax.
//------------------------------------------------------------------------------
bool
Acl::IsValid(const std::string& value, XrdOucErrInfo& error, bool is_sys_acl,
             bool check_numeric)
{
  // Empty is valid
  if (!value.length()) {
    return true;
  }

  int regexErrorCode;
  int result;
  regex_t regex;
  std::string regexString;

  if (is_sys_acl) {
    if (check_numeric) {
      regexString = sRegexSysNumericAcl;
    } else {
      regexString = sRegexSysGenericAcl;
    }
  } else {
    if (check_numeric) {
      regexString = sRegexUsrNumericAcl;
    } else {
      regexString = sRegexUsrGenericAcl;
    }
  }

  // Compile regex
  regexErrorCode = regcomp(&regex, regexString.c_str(), REG_EXTENDED);

  if (regexErrorCode) {
    error.setErrInfo(2, "failed to compile regex");
    regfree(&regex);
    return false;
  }

  // Execute regex
  result = regexec(&regex, value.c_str(), 0, NULL, 0);
  regfree(&regex);

  // Check the result
  if (result == 0) {
    return true;
  } else if (result == REG_NOMATCH) {
    error.setErrInfo(1, "invalid acl syntax");
    return false;
  } else { // REG_BADPAT, REG_ESPACE, etc...
    error.setErrInfo(2, "invalid regex or out of memory");
    return false;
  }
}

//------------------------------------------------------------------------------
// Convert acl rules to numeric uid/gid if needed
//------------------------------------------------------------------------------
void
Acl::ConvertIds(std::string& acl_val, bool to_string)
{
  if (acl_val.empty()) {
    return;
  }

  bool is_uid, is_gid;
  std::string sid;
  std::ostringstream oss;
  using eos::common::StringConversion;
  std::vector<std::string> rules;
  StringConversion::Tokenize(acl_val, rules, ",");

  if (!rules.size() && acl_val.length()) {
    rules.push_back(acl_val);
  }

  for (auto& rule : rules) {
    is_uid = is_gid = false;
    std::vector<std::string> tokens;
    StringConversion::Tokenize(rule, tokens, ":");
    eos_static_debug("rule=%s, tokens.size=%i", rule.c_str(), tokens.size());

    if (tokens.size() != 3) {
      oss << rule << ',';
      continue;
    }

    is_uid = (tokens[0] == "u");
    is_gid = (tokens[0] == "g");

    if (!is_uid && !is_gid) {
      oss << rule << ',';
      continue;
    }

    sid = tokens[1];
    bool needs_conversion = false;

    if (to_string) {
      // Convert to string representation if needed
      needs_conversion =
        (std::find_if(sid.begin(), sid.end(),
      [](const char& c) {
        return std::isalpha(c);
      }) == sid.end());
    } else {
      // Convert to numeric representation if needed
      needs_conversion =
        (std::find_if(sid.begin(), sid.end(),
      [](const char& c) {
        return std::isalpha(c);
      }) != sid.end());
    }

    if (needs_conversion) {
      int errc = 0;
      std::uint32_t numeric_id {0};
      std::string string_id {""};

      if (is_uid) {
        if (!to_string) {
          numeric_id = eos::common::Mapping::UserNameToUid(sid, errc);
          string_id = std::to_string(numeric_id);
        } else {
          numeric_id = atoi(sid.c_str());
          string_id = eos::common::Mapping::UidToUserName(numeric_id, errc);
        }
      } else {
        if (!to_string) {
          numeric_id = eos::common::Mapping::GroupNameToGid(sid, errc);
          string_id = std::to_string(numeric_id);
        } else {
          numeric_id = atoi(sid.c_str());
          string_id = eos::common::Mapping::GidToGroupName(numeric_id, errc);
        }
      }

      if (errc) {
        oss.str("");

        if (to_string) {
          oss << "failed to convert id: \"" << sid << "\" to string format";
        } else {
          oss << "failed to convert id: \"" << sid << "\" to numeric format";
        }

        // Print error message but still return the original value that we have
        eos_static_err(oss.str().c_str());
        string_id = sid;
      }

      oss << tokens[0] << ':' << string_id << ':' << tokens[2] << ',';
    } else {
      oss << rule << ',';
    }
  }

  acl_val = oss.str();

  if (*acl_val.rbegin() == ',') {
    acl_val.pop_back();
  }
}

EOSMGMNAMESPACE_END
