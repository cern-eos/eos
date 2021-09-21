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
#include "mgm/Share.hh"
#include "common/StringConversion.hh"
#include <regex.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Acl::Acl(std::string sysacl, std::string useracl, std::string shareacl,
         const eos::common::VirtualIdentity& vid, bool allowUserAcl)
{
  std::string tokenacl = TokenAcl(vid);
  Set(sysacl, useracl, shareacl, tokenacl, vid, allowUserAcl);
}



//------------------------------------------------------------------------------
//!
//! Constructor
//!
//! @param attr map containing all extended attributes
//! @param vid virtual id to match ACL
//!
//------------------------------------------------------------------------------

Acl::Acl(const eos::IContainerMD::XAttrMap& attrmap,
         const eos::common::VirtualIdentity& vid)
{
  // define the acl rules from the attributes
  SetFromAttrMap(attrmap, vid);
}

//------------------------------------------------------------------------------
// Constructor by path
//------------------------------------------------------------------------------
Acl::Acl(const char* path, XrdOucErrInfo& error,
         const eos::common::VirtualIdentity& vid,
         eos::IContainerMD::XAttrMap& attrmap, bool lockNs, bool asShare)
{
  if (path && strlen(path)) {
    int rc = gOFS->_attr_ls(path, error, vid, 0, attrmap, lockNs);

    if (rc) {
      eos_static_info("attr-ls failed: path=%s errno=%d", path, errno);
    }
  }

  // Set the acl rules from the attributes
  SetFromAttrMap(attrmap, vid, NULL, false, asShare);
}

//------------------------------------------------------------------------------
// Set Acls by interpreting the attribute map
//------------------------------------------------------------------------------
void
Acl::SetFromAttrMap(const eos::IContainerMD::XAttrMap& attrmap,
                    const eos::common::VirtualIdentity& vid, eos::IFileMD::XAttrMap* attrmapF,
                    bool sysaclOnly, bool asShare)
{
  bool evalUseracl = false;
  evaluserattrF = false;
  std::string sysacl;
  std::string useracl;
  std::string shareacl;
  std::string tokenacl;

  if (asShare) {
    auto it = attrmap.find("sys.share.acl");
    if (it != attrmap.end()) {
      sysacl = it->second;
    }
  } else {
    if (!sysaclOnly) {
      if (attrmapF != NULL && attrmapF->count("user.acl") > 0) {
	evalUseracl = attrmapF->count("sys.eval.useracl") > 0;
	if (evalUseracl) {
	  useracl = (*attrmapF)["user.acl"];
	  userattrF = useracl;
	  evaluserattrF = true;
	}
      } else {
	evalUseracl = attrmap.count("sys.eval.useracl") > 0;
	auto it = attrmap.find("user.acl");
	if (it != attrmap.end()) {
	  useracl = it->second;
	}
      }
    }

    tokenacl = TokenAcl(vid);
    auto it = attrmap.find("sys.acl");

    if (it != attrmap.end()) {
      sysacl = it->second;
    }

    it = attrmap.find("sys.acl.share");
    if (it != attrmap.end()) {
      shareacl = it->second;
    }
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("sysacl='%s' useracl='%s' shareacl='%s' tokenacl='%s' evalUseracl=%d asShare=%d",
                     sysacl.c_str(), useracl.c_str(), shareacl.c_str(), tokenacl.c_str(), evalUseracl, asShare);
  }

  Set(sysacl, useracl, shareacl, tokenacl, vid, evalUseracl);
}

//------------------------------------------------------------------------------
// Apply rules from a share ACL
//------------------------------------------------------------------------------

void
Acl::ApplyShare(Acl& acl)
{
  mHasAcl |= acl.HasAcl();
  mCanRead |= acl.CanRead();
  if (mCanRead) {
    mCanNotRead = false;
  }
  mCanWrite |= acl.CanWrite();
  if (mCanWrite) {
    mCanNotWrite = false;
  } else {
    mCanNotWrite |= acl.CanNotWrite();
  }
  mCanWriteOnce |= acl.CanWriteOnce();
  if (mCanWrite) {
    mCanWriteOnce = false;
  }
  mCanUpdate |= acl.CanUpdate();
  if (mCanUpdate) {
    mCanNotUpdate = false;
  } else {
    mCanNotUpdate |= acl.CanNotUpdate();
  }
  mCanBrowse |= acl.CanBrowse();
  if (mCanBrowse) {
    mCanNotBrowse = false;
  } else {
    mCanNotBrowse |= acl.CanNotBrowse();
  }

  mCanChmod |= acl.CanChmod();
  if (mCanChmod) {
    mCanNotChmod = false;
  } else {
    mCanNotChmod |= acl.CanNotChmod();
  }
  mCanChown |= acl.CanChown();
  mCanDelete |= acl.CanDelete();
  if (mCanDelete) {
    mCanNotDelete = false;
  }
  mCanSetQuota |= acl.CanSetQuota();
  mHasEgroup |= acl.HasEgroup();
  mIsMutable |= acl.IsMutable();
  mCanArchive |= acl.CanArchive();
  mCanPrepare |= acl.CanPrepare();
  mCanSetAcl |= acl.CanSetAcl();
  mCanShare |= acl.CanShare();
}

//------------------------------------------------------------------------------
// Set the contents of an ACL and compute the canXX and hasXX booleans.
//------------------------------------------------------------------------------
void
Acl::Set(std::string sysacl, std::string useracl, std::string shareacl, std::string tokenacl,
         const eos::common::VirtualIdentity& vid, bool allowUserAcl)
{
  std::string acl = "";
  sysattr = "";
  userattr = "";
  evaluserattr = false;

  if (sysacl.length()) {
    acl += sysacl;
    sysattr = sysacl;
  }

  if (shareacl.length()) {
    shareattr = shareacl;
  }

  if (allowUserAcl) {
    evaluserattr = true;
    if (useracl.length()) {
      if (sysacl.length()) {
        acl += ",";
      }

      acl += useracl;
      userattr = useracl;
    }
  }

  if (tokenacl.length()) {
    // overwrite all other ACLs with a token
    acl = tokenacl;
    sysacl = tokenacl;
    allowUserAcl = false;
  }

  // By default nothing is granted
  mHasAcl = false;
  mCanRead = false;
  mCanNotRead = false;
  mCanWrite = false;
  mCanNotWrite = false;
  mCanWriteOnce = false;
  mCanUpdate = false;
  mCanNotUpdate = false;
  mCanBrowse = false;
  mCanNotBrowse = false;
  mCanChmod = false;
  mCanNotChmod = false;
  mCanChown = false;
  mCanNotDelete = false;
  mCanDelete = false;
  mCanSetQuota = false;
  mCanArchive = false;
  mCanPrepare = false;
  mCanShare = false;
  mCanSetAcl = false;
  mHasEgroup = false;
  mIsMutable = true;

  // no acl definition
  if (!acl.length() && shareacl.empty()) {
    return;
  }

  int errc = 0;
  std::vector<std::string> rules;
  std::string delimiter = ",";
  eos::common::StringConversion::Tokenize(sysacl, rules, delimiter);
  int num_sysacl_rules =
    rules.size();     /* number of entries in sysacl, used to limit "+" (reallow) */

  if (allowUserAcl) {
    eos::common::StringConversion::Tokenize(useracl, rules,
                                            delimiter);  /* append to rules */
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("sysacl '%s' (%d entries), useracl '%s', total %d entries",
                     sysacl.c_str(), num_sysacl_rules, useracl.c_str(), rules.size());
  }

  std::vector<std::string>::const_iterator it;
  XrdOucString sizestring1;
  XrdOucString sizestring2;
  char denials[256], reallows[256];
  memset(denials, 0, sizeof(denials));        /* start with no denials */
  memset(reallows, 0, sizeof(reallows));      /* nor reallows */

  for (const auto& chk_gid : vid.allowed_gids) {
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

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("username '%s' groupname '%s'", username.c_str(),
                       groupname.c_str());
    }

    std::string usr_name_tag = "u:";
    usr_name_tag += username;
    usr_name_tag += ":";
    std::string grp_name_tag = "g:";
    grp_name_tag += groupname;
    grp_name_tag += ":";
    std::string ztag = "z:";
    std::string keytag = "k:";
    keytag += vid.key;
    keytag += ":";;

    if (EOS_LOGS_DEBUG) eos_static_debug("%s %s %s %s %s", usertag.c_str(),
                                           grouptag.c_str(),
                                           usr_name_tag.c_str(), grp_name_tag.c_str(), keytag.c_str());

    // Rule interpretation logic
    int sysacl_rules_remaining = num_sysacl_rules;

    for (it = rules.begin(); it != rules.end(); it++) {
      bool egroupmatch = false;
      /* when negative, we're in user.acl */
      sysacl_rules_remaining -= 1;

      // Check for e-group membership
      if (!it->compare(0, strlen("egroup:"), "egroup:")) {
        std::vector<std::string> entry;
        std::string delimiter = ":";
        eos::common::StringConversion::Tokenize(*it, entry, delimiter);

        if (entry.size() < 3) {
          continue;
        }

        egroupmatch = gOFS->EgroupRefresh->Member(username, entry[1]);
        mHasEgroup = egroupmatch;
      }

      // Match 'our' rule
      if ((!it->compare(0, usertag.length(), usertag)) ||
          (!it->compare(0, grouptag.length(), grouptag)) ||
          (!it->compare(0, ztag.length(), ztag)) ||
          (egroupmatch) ||
          (!it->compare(0, keytag.length(), keytag)) ||
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

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("parsing permissions '%s'", entry[2].c_str());
        }

        bool deny = false, reallow = false;

        for (char* s = (char*) entry[2].c_str(); s[0] != 0; s++) {
          int c = s[0];            /* need a copy in case of an "s++" later */

          if (EOS_LOGS_DEBUG) {
            eos_static_debug("c=%c deny=%d reallow=%d", c, deny, reallow);
          }

          if (reallow && !(c == 'u' || c == 'd')) {
            eos_static_info("'+' Acl flag ignored for '%c'", c);
          }

          switch (c) {
          case '!':
            deny = true;
            continue;

          case '+':
            reallow = true;
            continue;

          case 'a': // 'a' defines archiving permission
            mCanArchive = !deny;
            break;

	  case 'e': // 'e' defines set acl permission
	    mCanSetAcl = !deny;
	    break;

          case 'r': // 'r' defines read permission
            mCanRead = !deny;
            break;

	  case 's': // 's' defines sharing permission
	    mCanShare = !deny;
	    break;

          case 'x': // 'x' defines browsing permission
            mCanBrowse = !deny;
            break;

          case 'p': // 'p' defines workflow permission
            mCanPrepare = !deny;
            break;

          case 'm': // 'm' defines mode change permission
            if (deny) {
              mCanNotChmod = true;
            } else {
              mCanChmod = true;
            }

            break;

          case 'c': // 'c' defines owner change permission (for directories)
            /* pass here; but chown imposes further restrictions, like limited to sys.acl */
            mCanChown = true;
            break;

          case 'd': // '!d' forbids deletion
            if (deny && !mCanDelete) {
              mCanNotDelete = true;
            } else if (reallow) {
              if (sysacl_rules_remaining < 0) {
                eos_static_info("'+d' ignored in user acl '%s'", entry[2].c_str());
                reallow = 0;        /* ignore the reallow */
                break;
              }

              mCanDelete = true;
              mCanNotDelete = false;
              mCanWriteOnce = false;
              denials['d'] = 0;               /* drop denial, 'd' and 'u' are "odd" */
            }

            break;

          case 'u':// '!u' denies update, 'u' and '+u' add update. '!+u' and '+!u' would *deny* updates
            mCanUpdate = !deny;

            if (mCanUpdate && reallow) {
              denials['u'] = 0;  /* drop denial, 'd' and 'u' are "odd" */
            }

            break;

          case 'w': // 'wo' defines write once permissions, 'w' defines write permissions if 'wo' is not granted
            if ((s + 1)[0] == 'o') {  /* this is a 'wo' */
              s++;
              c = 'W';        /* for the denial entry */
              mCanWriteOnce = !deny;
            } else {
              if (!mCanWriteOnce) {
                mCanWrite = !deny;
                mCanUpdate = !deny; // by default 'w' adds update rights
              }
            }

            break;

          case 'q':
            if (sysacl_rules_remaining >=
                0) { // this is only valid if specified as a sysacl
              mCanSetQuota = !deny;
            }

            break;

          case 'i': // 'i' makes directories immutable
            mIsMutable = deny;
            break;
          }

          mHasAcl = true;

          if (reallow) {
            reallows[c] = 1;    /* remember reallows */
          } else if (deny) {
            denials[c] = 1;     /* remember denials */
          }

          deny = reallow = false;           /* reset for next permission char */
        }
      }
    }
  }

  /* Now that all ACLs have been parsed, handle re-allows and denials */
  char rights[] = "arxpmcWwdui";
  unsigned char r;

  for (int i = 0; (r = rights[i]); i++) {
    bool is_allowed;

    if (reallows[r]) {
      denials[r] = 0;
      is_allowed = true;
      eos_static_debug("reallow %c", r);
    } else if (denials[r]) {        /* re-allows beat denials */
      is_allowed = false;

      if (r != 'W') {
        eos_static_debug("deny %c", r);
      }
    } else {
      continue;
    }

    switch (r) {
    case 'a':
      mCanArchive = is_allowed;
      break;

    case 'e':
      mCanSetAcl = is_allowed;
      break;

    case 's':
      mCanShare = is_allowed;
      break;

    case 'r':
      mCanRead = is_allowed;
      mCanNotRead = !is_allowed;
      break;

    case 'x':
      mCanBrowse = is_allowed;
      mCanNotBrowse = !is_allowed;
      break;

    case 'p':
      mCanPrepare = is_allowed;
      break;

    case 'm':
      mCanNotChmod = !is_allowed;
      break;

    case 'c':
      mCanChown = is_allowed;
      break;

    case 'W':
      mCanWriteOnce = is_allowed;
      eos_static_debug("writeonce %d", mCanWriteOnce);
      break;

    case 'w':
      mCanWrite = is_allowed;
      mCanNotWrite = !is_allowed;

      /* if mCanWrite, grant mCanUpdate implicitely unless 'u' explicitely denied */
      if (mCanWrite) {
        mCanUpdate = true;  /* 'u' is checked after 'w', this could be reverted */
      }

      break;

    case 'd':
      mCanNotDelete = !is_allowed;
      break;

    case 'u':
      mCanUpdate = is_allowed;
      mCanNotUpdate = !is_allowed;
      break;

    case 'i':
      mIsMutable = !is_allowed;
      break;
    }
  }

  if (shareacl.length()) {
    // now get all the shares
    std::vector<std::string> rules;
    std::string delimiter = ",";

    eos::common::StringConversion::Tokenize(shareacl, rules, delimiter);
    eos_static_debug("to parse %s\n", shareacl.c_str());
    for ( auto i : rules ) {
      eos_static_debug("parsing rule %s\n", i.c_str());
      eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
      std::shared_ptr<eos::mgm::Acl> s_acl = Share::getShareAclById(vid, i);
      if (s_acl) {
	ApplyShare(*s_acl);
      }
    }
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug(
      "mCanRead %d mCanNotRead %d mCanWrite %d mCanNotWrite %d mCanWriteOnce %d mCanUpdate %d mCanNotUpdate %d "
      "mCanBrowse %d mCanNotBrowse %d mCanChmod %d mCanChown %d mCanNotDelete %d mCanNotChmod %d "
      "mCanDelete %d mCanSetQuota %d mHasAcl %d mHasEgroup %d mIsMutable %d mCanArchive %d mCanPrepare %d mCanSetAcl %d mCanShare %d",
      mCanRead, mCanNotRead, mCanWrite, mCanNotWrite, mCanWriteOnce, mCanUpdate,
      mCanNotUpdate,
      mCanBrowse, mCanNotBrowse, mCanChmod, mCanChown, mCanNotDelete, mCanNotChmod,
      mCanDelete, mCanSetQuota, mHasAcl, mHasEgroup, mIsMutable, mCanArchive,
      mCanPrepare, mCanSetAcl, mCanShare);
  }
}

//------------------------------------------------------------------------------
// Return AclType for a given extended attribute name
//------------------------------------------------------------------------------
Acl::AclType
Acl::GetType(const std::string& value)
{
  if (value == "sys.acl") {
    return Acl::eSysAcl;
  }
  if (value == "user.acl") {
    return Acl::eUserAcl;
  }
  if (value == "sys.acl.share") {
    return Acl::eShareAcl;
  }
  if (value == "sys.share.acl") {
    return Acl::eSysAcl;
  }
  return Acl::eNoAcl;
}

//------------------------------------------------------------------------------
// Check whether ACL has a valid format / syntax.
//------------------------------------------------------------------------------
bool
Acl::IsValid(const std::string& value, XrdOucErrInfo& error, Acl::AclType type,
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

  switch (type) {
  case eSysAcl:
    if (check_numeric) {
      regexString = sRegexSysNumericAcl;
    } else {
      regexString = sRegexSysGenericAcl;
    }
    break;
  case eUserAcl:
    if (check_numeric) {
      regexString = sRegexUsrNumericAcl;
    } else {
      regexString = sRegexUsrGenericAcl;
    }
  case eShareAcl:
    regexString = sRegexShareAcl;
    break;
  case eNoAcl:
    error.setErrInfo(2, "invalid extended attribute");
    return false;
  }

  // Compile regex
  regexErrorCode = regcomp(&regex, regexString.c_str(), REG_EXTENDED);

  if (regexErrorCode) {
    eos_static_debug("regcomp regexErrorCode=%d regex '%s'", regexErrorCode,
                     regexString.c_str());      // the setErrInfo below does not always produce a visible result
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
int
Acl::ConvertIds(std::string& acl_val, bool to_string)
{
  if (acl_val.empty()) {
    return 0;
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
        return 1;
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

  return 0;
}


//------------------------------------------------------------------------------
// Extract an ACL rule from a token
//------------------------------------------------------------------------------

std::string
Acl::TokenAcl(const eos::common::VirtualIdentity& vid) const
{
  if (vid.token) {
    if (vid.token->Valid()) {
      if (!vid.token->ValidatePath(vid.scope)) {
        std::string tokenacl;
        tokenacl = "u:";
        tokenacl += vid.uid_string;
        tokenacl += ":";
        tokenacl += vid.token->Permission();
        return tokenacl;
      } else {
	eos_static_err("invald path token");
      }
    } else {
      eos_static_err("invalid token");
    }
  } else {
    eos_static_debug("no token");
  }

  return "";
}

//------------------------------------------------------------------------------
// Output ACL state
std::string
Acl::Out(bool monitoring, accessmap_t* map)
{
  std::string format_s = !monitoring ? "s" : "os";
  TableFormatterBase table_all;

  if (!monitoring) {
    table_all.SetHeader({
	  std::make_tuple("op", 16, format_s),
	  std::make_tuple("perm", 8, format_s),
          });
  } else {
    table_all.SetHeader({
          std::make_tuple("op", 0, format_s),
	  std::make_tuple("perm", 0, format_s),
          });
  }

  {
    // can read
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("read"), format_s));
    table_data.back().push_back(TableCell(mCanRead?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["read"] = mCanRead;
  }

  {
    // can not read
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("not-read"), format_s));
    table_data.back().push_back(TableCell(mCanNotRead?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["notread"] = mCanNotRead;
  }

  {
    // can write
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("write"), format_s));
    table_data.back().push_back(TableCell(mCanWrite?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["write"] = mCanWrite;
  }

  {
    // can not write
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("not-write"), format_s));
    table_data.back().push_back(TableCell(mCanNotWrite?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["notwrite"] = mCanNotWrite;
  }

  {
    // can write-once
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("write-once"), format_s));
    table_data.back().push_back(TableCell(mCanWriteOnce?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["writeonce"] = mCanWriteOnce;
  }

  {
    // can update
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("update"), format_s));
    table_data.back().push_back(TableCell(mCanUpdate?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["update"] = mCanUpdate;
  }

  {
    // can not update
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("not-update"), format_s));
    table_data.back().push_back(TableCell(mCanNotUpdate?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["notupdate"] = mCanNotUpdate;
  }

  {
    // can browse
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("browse"), format_s));
    table_data.back().push_back(TableCell(mCanBrowse?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["browse"] = mCanBrowse;
  }

  {
    // can not browse
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("not-browse"), format_s));
    table_data.back().push_back(TableCell(mCanNotBrowse?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["notbrowse"] = mCanNotBrowse;
  }

  {
    // can chmod
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("chmod"), format_s));
    table_data.back().push_back(TableCell(mCanChmod?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["chmod"] = mCanChmod;
  }

  {
    // can not chmod
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("not-chmod"), format_s));
    table_data.back().push_back(TableCell(mCanNotChmod?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["notchmod"] = mCanNotChmod;
  }

  {
    // can chown
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("chown"), format_s));
    table_data.back().push_back(TableCell(mCanChown?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["chown"] = mCanChown;
  }

  {
    // can delete
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("delete"), format_s));
    table_data.back().push_back(TableCell(mCanDelete?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["delete"] = mCanDelete;
  }

  {
    // can not delete
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("not-delete"), format_s));
    table_data.back().push_back(TableCell(mCanNotDelete?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["notdelete"] = mCanNotDelete;
  }

  {
    // can set quota
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("set-quota"), format_s));
    table_data.back().push_back(TableCell(mCanSetQuota?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["setquota"] = mCanSetQuota;
  }

  {
    // can archive
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("archive"), format_s));
    table_data.back().push_back(TableCell(mCanArchive?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["archive"] = mCanArchive;
  }

  {
    // can prepare
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("prepare"), format_s));
    table_data.back().push_back(TableCell(mCanPrepare?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["prepare"] = mCanPrepare;
  }

  {
    // can share
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("share"), format_s));
    table_data.back().push_back(TableCell(mCanShare?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["share"] = mCanShare;
  }

  {
    // can set acl
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("set-acl"), format_s));
    table_data.back().push_back(TableCell(mCanSetAcl?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["setacl"] = mCanSetAcl;
  }

  {
    // has egroup
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("egroup"), format_s));
    table_data.back().push_back(TableCell(mHasEgroup?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["egroup"] = mHasEgroup;
  }

  {
    // is mutable
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell(std::string("mutable"), format_s));
    table_data.back().push_back(TableCell(mIsMutable?std::string("yes"):std::string("no"), format_s));
    table_all.AddRows(table_data);
    if (map) (*map)["mutable"] = mIsMutable;
  }

 return table_all.GenerateTable(HEADER);
}
EOSMGMNAMESPACE_END
