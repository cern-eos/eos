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
#include "namespace/utils/Attributes.hh"
#include "namespace/interface/IView.hh"
#include "common/StringConversion.hh"
#include <regex.h>

EOSMGMNAMESPACE_BEGIN

/* initially -1, will be set from config on first use:
   1 = file modebits work like in Posix
   0 = file modebits ignored like in AFS
*/
int Acl::file_modebits_posix = -1;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Acl::Acl(std::string sysacl, std::string useracl,
         const eos::common::VirtualIdentity& vid, bool allowUserAcl,
         uid_t uid, gid_t gid, mode_t modebits)
{
  std::string tokenacl = TokenAcl(vid);
  Set(sysacl, useracl, tokenacl, vid, allowUserAcl, uid, gid, modebits);
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
         const eos::common::VirtualIdentity& vid,
         uid_t uid, gid_t gid, mode_t modebits)
{
  // define the acl rules from the attributes
  SetFromAttrMap(attrmap, vid, false, NULL, uid, gid, modebits);
}

//------------------------------------------------------------------------------
//!
//! Constructor
//!
//! @param std::shared_ptr<eos::IContainerMD> ptr to container
//! @param std::shared_ptr<eos::IFileMD> ptr to file or NULL
//! @param vid virtual id to match ACL
//!
//------------------------------------------------------------------------------

Acl::Acl(std::shared_ptr<eos::IContainerMD> cmd,
         std::shared_ptr<eos::IFileMD> fmd,
         const eos::common::VirtualIdentity& vid,
         bool allowUserAcl) {

    eos::IFileMD::XAttrMap attrmapF;
    if (fmd != nullptr) attrmapF = fmd->getAttributes();

    SetFromAttrMap(cmd->getAttributes(), vid, !allowUserAcl,
        (fmd != nullptr) ? &attrmapF : NULL,
        (fmd != nullptr) ? fmd->getCUid() : cmd->getCUid(),
        (fmd != nullptr) ? fmd->getCGid() : cmd->getCGid(),
        (fmd != nullptr && Acl::file_modebits_posix==1) ?
            fmd->getFlags() : cmd->getMode());
}
//------------------------------------------------------------------------------
// Constructor by path
//------------------------------------------------------------------------------
Acl::Acl(const char* path, XrdOucErrInfo& error,
         const eos::common::VirtualIdentity& vid,
         eos::IContainerMD::XAttrMap& attrmap, bool lockNs)
{
  if (path && strlen(path)) {
    int rc = 0;
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();
    if (item.file) {
        file_uid = item.file->getCUid();
        file_gid = item.file->getCGid();
        mbits = item.file->getFlags();
        listAttributes(gOFS->eosView, item.file.get(), attrmap, false);
        if (Acl::file_modebits_posix != 1 || attrmap.count("sys.acl") == 0 ||
                attrmap.count("user.acl") == 0) {
            std::shared_ptr<eos::IContainerMD> cmd = gOFS->eosDirectoryService->getContainerMD(item.file->getContainerId());
            eos::IContainerMD::XAttrMap attrmapC;
            listAttributes(gOFS->eosView, cmd.get(), attrmapC, false);

            if (Acl::file_modebits_posix != 1) mbits = cmd->getMode();
            if (attrmap.count("sys.acl") == 0 && attrmapC.count("sys.acl") > 0)
                attrmap["sys.acl"] = attrmapC["sys.acl"];
            if (attrmap.count("user.acl") == 0 && attrmapC.count("user.acl") > 0)
                attrmap["user.acl"] = attrmapC["user.acl"];
        }
    } else if (item.container) {
        file_uid = item.container->getCUid();
        file_gid = item.container->getCGid();
        mbits = item.container->getMode();
        listAttributes(gOFS->eosView, item.container.get(), attrmap, false);
    }
        
    if (rc) {
      eos_static_info("attr-ls failed: path=%s errno=%d", path, errno);
    }
  }

  // Set the acl rules from the attributes
  SetFromAttrMap(attrmap, vid, false, NULL, file_uid, file_gid, mbits);
}

//------------------------------------------------------------------------------
// Set Acls by interpreting the attribute map
//------------------------------------------------------------------------------
void
Acl::SetFromAttrMap(const eos::IContainerMD::XAttrMap& attrmap,
                    const eos::common::VirtualIdentity& vid, bool sysaclOnly,
                    eos::IFileMD::XAttrMap* attrmapF,
                    uid_t uid, gid_t gid, mode_t fmodebits)
{
  bool evalUseracl = false;
  std::string useracl;
  std::string tokenacl;

  if (!sysaclOnly) {
    if (attrmapF != NULL && attrmapF->count("user.acl") > 0) {
      evalUseracl = attrmapF->count("sys.eval.useracl") > 0;

      if (evalUseracl) {
        useracl = (*attrmapF)["user.acl"];
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
  std::string sysAcl;
  auto it = attrmap.find("sys.acl");

  if (it != attrmap.end()) {
    sysAcl = it->second;
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("sysacl='%s' useracl='%s' tokenacl='%s'  evalUseracl=%d vid=%d/%d %d/%d/%#o",
                     sysAcl.c_str(), useracl.c_str(), tokenacl.c_str(), evalUseracl,
                     vid.uid, vid.gid, uid, gid, fmodebits);
  }

  Set(sysAcl, useracl, tokenacl, vid, evalUseracl, uid, gid, fmodebits);
}

//------------------------------------------------------------------------------
// Set the contents of an ACL and compute the canXX and hasXX booleans.
//------------------------------------------------------------------------------
void
Acl::Set(std::string sysacl, std::string useracl, std::string tokenacl,
         const eos::common::VirtualIdentity& vid, bool allowUserAcl,
         uid_t uid, gid_t gid, mode_t fmodebits)
{
  if (Acl::file_modebits_posix == -1) { /* initially */
    int val = 0;
    char *s = getenv("EOS_FILE_MODEBITS_POSIX");
    if (s) val = atoi(s);
    Acl::file_modebits_posix = (val > 0) ? 1 /* Posix-like*/ : 0 /* AFS-like */;
    eos_static_info("file_modebits_posix=%d", Acl::file_modebits_posix);
  }

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

  if (tokenacl.length()) {
    // overwrite all other ACLs with a token
    acl = tokenacl;
    sysacl = tokenacl;
    allowUserAcl = false;
  }

  mbits = fmodebits;
  file_uid = uid;
  file_gid = gid;

  // By default nothing is granted
  mCanNotRead = false;
  mHasAcl = false;
  mCanNotWrite = false;
  mCanNotUpdate = false;
  mCanWriteOnce = false;
  mCanNotStat = false;
  mCanNotChmod = false;
  mCanNotDelete = false;
  mHasEgroup = false;
  mIsMutable = true;

  if (vid.uid == 0) {                       // root can do anything
    mCanRead = true;
    mCanWrite = true;
    mCanUpdate = true;
    mCanStat = true;
    mCanChmod = true;
    mCanChown = true;
    mCanDelete = true;
    mCanSetQuota = true;
    mCanArchive = true;
    mCanPrepare = true;
    return;
  }

  mCanRead = false;
  mCanWrite = false;
  mCanUpdate = false;
  mCanStat = false;
  mCanChmod = false;
  mCanChown = false;
  mCanDelete = false;
  mCanSetQuota = false;
  mCanArchive = false;
  mCanPrepare = false;

  bool saw_u = false, saw_g = false, saw_o = false;

  int errc = 0;
  std::vector<std::string> rules;
  std::string delimiter = ",";
  eos::common::StringConversion::Tokenize(sysacl, rules, delimiter);
  int num_sysacl_rules =
    rules.size();     /* number of entries in sysacl, used to limit "+" (reallow) */

  if (allowUserAcl && acl.length()) {
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


  if (acl.length() > 0) {

    std::string userid = eos::common::StringConversion::GetSizeString(sizestring1,
                         (unsigned long long) vid.uid);
    std::string usertag = "u:";
    usertag += userid;
    usertag += ":";
    std::string username = eos::common::Mapping::UidToUserName(vid.uid, errc);

    if (errc) {
      username = "_INVAL_";
    }

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("username '%s'", username.c_str());
    }

    std::string usr_name_tag = "u:";
    usr_name_tag += username;
    usr_name_tag += ":";
    std::string ztag = "z:";
    std::string keytag = "k:";
    keytag += vid.key;
    keytag += ":";;

    if (EOS_LOGS_DEBUG)
        eos_static_debug("%s %s %s", usertag.c_str(), usr_name_tag.c_str(), keytag.c_str());

    // Rule interpretation logic
    int sysacl_rules_remaining = num_sysacl_rules;

    for (it = rules.begin(); it != rules.end(); it++) {
      bool egroupmatch = false, groupmatch = false;
      sysacl_rules_remaining -= 1;              /* when negative, we're in user.acl */

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
      else if (!it->compare(0, 2, "u:")) saw_u = true;
      else if (!it->compare(0, 2, "g:")) {
        unsigned int colonPos = it->find(':', 2);
        if (colonPos == string::npos) continue;     /* invalid g: specification */

        saw_g = true;
        std::string grp = it->substr(2, colonPos-2);    /*g:1:rights*/
        gid_t gid;

        if (isdigit(grp.at(0))) gid = std::stoul(grp);
        else {
            gid = eos::common::Mapping::GroupNameToGid(grp, errc);
            eos_static_info("ACL resolving group '%s', err=%d", grp.c_str(), errc);
            if (errc) {                             /* invalid group name */
                continue;
            }
        }
        
        groupmatch = gid == vid.gid;
        if (not groupmatch) {
            for (const auto& chk_gid2 : vid.allowed_gids) {
                if ((groupmatch = (chk_gid2 == gid))) break;
            }
        }
        if (not groupmatch) continue;               /* entry not applicable */
      }



      // Match 'our' rule
      if (egroupmatch || groupmatch ||
          (!it->compare(0, usertag.length(), usertag)) ||
          (!it->compare(0, ztag.length(), ztag)) ||
          (!it->compare(0, keytag.length(), keytag)) ||
          (!it->compare(0, usr_name_tag.length(), usr_name_tag))) {

        std::vector<std::string> entry;
        std::string delimiter = ":";
        eos::common::StringConversion::Tokenize(*it, entry, delimiter);

        if (entry.size() < 3) {
          if (it->at(0) == 'z') {   // z tag entries have only two fields
            saw_o = true;
            entry.resize(3);
            entry[2] = entry[1];    // rights are 3rd field, "" if there are no rights
          } else
            // other entries may have no rights, they'd be ignored
            continue;
        }

        //if (EOS_LOGS_DEBUG)
        //    eos_static_debug("parsing permissions '%s'", entry[2].c_str());

        bool deny = false, reallow = false;

        for (char* s = (char*) entry[2].c_str(); s[0] != 0; s++) {
          int c = s[0];            /* need a copy in case of an "s++" later */

          //if (EOS_LOGS_DEBUG)
          //  eos_static_debug("c=%c deny=%d reallow=%d", c, deny, reallow);

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

          case 'r': // 'r' defines read permission
            mCanRead = !deny;
            break;

          case 'x': // 'x' defines stat permission
            mCanStat = !deny;
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
                mCanDelete = !deny; // normally 'w' implies 'd'
                // remember the denial/reallow for "update" as well! What about "delete"?
                if (deny) {
                    denials['u'] = 1;
                    // what about !w in a directory? Does this imply !d ??
                }
                if (reallow) {
                    reallows['u'] = 1;
                }
            
              }
            }

            break;

          case 'q':
            if (sysacl_rules_remaining >= 0) {  // only valid if specified as a sysacl
              mCanSetQuota = !deny;
            }

            break;

          case 'i': // 'i' makes directories immutable
            mIsMutable = deny;
            break;
          }

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

  /* for entities not in any ACL entry, fall back to mode bits */
  mode_t modebits_mask = S_ISVTX | S_ISGID;      /* initial mask: keep special bits */;

  if (!saw_u && file_uid == vid.uid) modebits_mask |= S_IRWXU;   /* add u (owner) rights */
  if (!saw_g) {
      if (file_gid == vid.gid) modebits_mask |= S_IRWXG;
      else
          for (const auto& chk_gid : vid.allowed_gids) {
              if (file_gid == chk_gid) {
                  modebits_mask |= S_IRWXG;
                  break;
              }
          }
  }
  if (!saw_o) modebits_mask |= S_IRWXO;
  
  mode_t modebits_eff = modebits_mask & fmodebits;              /* resulting mode bits */

  if (EOS_LOGS_DEBUG) eos_static_debug("saw_u %d saw_g %d saw_o %d uid/gid %d/%d "
    "fuid/gid %d/%d modebits_mask=%#o fmodebits=%#o modebits_eff=%#o",
    saw_u, saw_g, saw_o, vid.uid, vid.gid, file_uid, file_gid,
    modebits_mask, fmodebits, modebits_eff);

  if (modebits_eff & (S_IWUSR|S_IWGRP|S_IWOTH)) {
    mCanWrite = true;
    mCanUpdate = true;
    /* the following is:
    if (modebits_eff & S_ISVTX) mCanDelete = modebits_eff & S_IWUSR;
    else mcanDelete = true; */
    mCanDelete = ( (modebits_eff & (S_IWUSR|S_ISVTX)) == (S_IWUSR|S_ISVTX) );
  }
  if (modebits_eff & (S_IRUSR|S_IRGRP|S_IROTH)) mCanRead = true;
  if (modebits_eff & (S_IXUSR|S_IXGRP|S_IXOTH)) mCanStat = true;

  /* Now that all ACLs have been parsed, handle re-allows and denials */
  static char rights[] = "arxpmcWwdui";
  unsigned char r;

  if (acl.length() > 0) {
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

    case 'r':
      mCanRead = is_allowed;
      mCanNotRead = !is_allowed;
      break;

    case 'x':
      mCanStat = is_allowed;
      mCanNotStat = !is_allowed;
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
  }

  mHasAcl = true;

  if (EOS_LOGS_DEBUG) {
    eos_static_debug(
      "mCanRead %d mCanNotRead %d mCanWrite %d mCanNotWrite %d mCanWriteOnce %d mCanUpdate %d mCanNotUpdate %d "
      "mCanStat %d mCanNotStat %d mCanChmod %d mCanChown %d mCanNotDelete %d mCanNotChmod %d "
      "mCanDelete %d mCanSetQuota %d mHasAcl %d mHasEgroup %d mIsMutable %d mCanArchive %d mCanPrepare %d",
      mCanRead, mCanNotRead, mCanWrite, mCanNotWrite, mCanWriteOnce, mCanUpdate,
      mCanNotUpdate,
      mCanStat, mCanNotStat, mCanChmod, mCanChown, mCanNotDelete, mCanNotChmod,
      mCanDelete, mCanSetQuota, mHasAcl, mHasEgroup, mIsMutable, mCanArchive,
      mCanPrepare);
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
      }
    }
  }

  return "";
}


EOSMGMNAMESPACE_END
