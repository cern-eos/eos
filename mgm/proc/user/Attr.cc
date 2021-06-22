// ----------------------------------------------------------------------
// File: proc/user/Attr.cc
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

#include "common/Path.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Resolver.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Make sure the input given by the client makes sense
//!
//! @return true if successful, otherwise false
//------------------------------------------------------------------------------
bool SanitizeXattr(const std::string& key, const std::string& value)
{
  if ((key == "sys.forced.blocksize") || (key == "user.forced.blocksize")) {
    std::string out_val;
    (void)eos::common::SymKey::DeBase64(value, out_val);
    return eos::common::LayoutId::IsValidBlocksize(out_val);
  }

  return true;
}

int
ProcCommand::Attr()
{
  XrdOucString spath = pOpaque->Get("mgm.path");
  XrdOucString option = pOpaque->Get("mgm.option");
  const char* inpath = spath.c_str();
  uint64_t identifier = 0;
  NAMESPACEMAP;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  if ((spath.beginswith("fid:") || spath.beginswith("fxid:"))) {
    WAIT_BOOT;
    identifier = Resolver::retrieveFileIdentifier(spath)
                 .getUnderlyingUInt64();
    spath = "";
    GetPathFromFid(spath, identifier, "error: ");
  } else if ((spath.beginswith("pid:") || spath.beginswith("pxid:"))) {
    WAIT_BOOT;
    spath.replace('p', 'f', 0, 1);
    identifier = Resolver::retrieveFileIdentifier(spath)
                 .getUnderlyingUInt64();
    spath = "";
    GetPathFromCid(spath, identifier, "error: ");
  } else {
    spath = eos::common::Path(path).GetPath();

    while (spath.replace("#AND#", "&")) {}
  }

  path = spath.c_str();
  PROC_TOKEN_SCOPE;

  if ((!spath.length()) && (!identifier)) {
    // Empty path or invalid numeric identifier
    stdErr = "error: you have to give a valid identifier (<path>|fid:<fid-dec>|fxid:<fid-hex>|pid:<pid-dec>|pxid:<pid-hex>)";
    retc = EINVAL;
  } else if ((!spath.length())) {
    // Retrieval of path from numeric identifier failed
    retc = errno;
  } else if ((mSubCmd != "set") && (mSubCmd != "get") && (mSubCmd != "ls") &&
             (mSubCmd != "rm") && (mSubCmd != "fold")) {
    // Unrecognized subcommand
    stdErr = "error: the subcommand must be one of 'ls', 'get', 'set', 'rm' or 'fold'!";
    retc = EINVAL;
  } else {
    if (((mSubCmd == "set") && ((!pOpaque->Get("mgm.attr.key")) ||
                                ((!pOpaque->Get("mgm.attr.value"))))) ||
        ((mSubCmd == "get") && ((!pOpaque->Get("mgm.attr.key")))) ||
        ((mSubCmd == "rm") && ((!pOpaque->Get("mgm.attr.key"))))) {
      stdErr = "error: you have to provide 'mgm.attr.key' for set,get,rm and 'mgm.attr.value' for set commands!";
      retc = EINVAL;
    } else {
      retc = 0;
      XrdOucString key = pOpaque->Get("mgm.attr.key");
      XrdOucString val = pOpaque->Get("mgm.attr.value");

      while (val.replace("\"", "")) {
      }

      if (val.length() && !SanitizeXattr(key.c_str(), val.c_str())) {
        stdErr = "error: invalid input";
        retc = EINVAL;
        return SFS_OK;
      }

      // Find everything to be modified i.e. directories only
      std::map<std::string, std::set<std::string> > found;

      if (option == "r") {
        if (gOFS->_find(spath.c_str(), *mError, stdErr, *pVid, found, nullptr,
                        nullptr, true)) {
          stdErr += "error: unable to search in path";
          retc = errno;
        }
      } else {
        // the single dir case
        (void) found[spath.c_str()].size();
      }

      if (!retc) {
        // apply to  directories starting at the highest level
        for (auto foundit = found.begin(); foundit != found.end(); foundit++) {
          {
            eos::IContainerMD::XAttrMap map;
            eos::IContainerMD::XAttrMap linkmap;

            if ((mSubCmd == "ls")) {
              if (gOFS->_access(foundit->first.c_str(), R_OK, *mError, *pVid, 0)) {
                stdErr += "error: unable to get attributes  ";
                stdErr += foundit->first.c_str();
                retc = errno;
                return SFS_OK;
              }

              XrdOucString partialStdOut = "";

              if (gOFS->_attr_ls(foundit->first.c_str(), *mError, *pVid, (const char*) 0, map,
                                 true, true)) {
                stdErr += "error: unable to list attributes of ";
                stdErr += foundit->first.c_str();
                stdErr += "\n";
                retc = errno;
              } else {
                eos::IContainerMD::XAttrMap::const_iterator it;

                if (option == "r") {
                  stdOut += foundit->first.c_str();
                  stdOut += ":\n";
                }

                for (it = map.begin(); it != map.end(); ++it) {
                  partialStdOut += (it->first).c_str();

                  if (it->first != "sys.file.buffer") {
                    partialStdOut += "=";
                    partialStdOut += "\"";
                    partialStdOut += (it->second).c_str();
                  } else {
                    partialStdOut += "=\"[";
                    partialStdOut += (std::to_string(it->second.size()).c_str());
                    partialStdOut += "] bytes";
                  }

                  partialStdOut += "\"";
                  partialStdOut += "\n";
                }

                stdOut += partialStdOut;

                if (option == "r") {
                  stdOut += "\n";
                }
              }
            }

            if (mSubCmd == "set") {
              if (key == "user.acl") {
                XrdOucString evalacl;

                // If someone wants to set a user.acl and the tag sys.eval.useracl
                // is not there, we return an error ...
                if ((pVid->uid != 0) && gOFS->_attr_get(foundit->first.c_str(),
                                                        *mError, *pVid,
                                                        (const char*) 0,
                                                        "sys.eval.useracl", evalacl)) {
                  stdErr += "error: unable to set user.acl - the file/directory does not "
                            "evaluate user acls (sys.eval.useracl is undefined)!\n";
                  retc = EINVAL;
                  return SFS_OK;
                }
              }

              // Check if the origin exists and is a directory
              if (key == "sys.attr.link") {
                eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

                try {
                  auto cmd = gOFS->eosView->getContainer(val.c_str());
                } catch (eos::MDException& e) {
                  std::ostringstream oss;
                  oss << "error: " << val.c_str()
                      << " must be an existing directory" << std::endl;
                  stdErr = oss.str().c_str();
                  retc = EINVAL;
                  return SFS_OK;
                }
              }

              if (gOFS->_attr_set(foundit->first.c_str(), *mError, *pVid, (const char*) 0,
                                  key.c_str(), val.c_str())) {
                stdErr += "error: unable to set attribute in file/directory ";
                stdErr += foundit->first.c_str();

                if (mError != 0) {
                  stdErr += ": ";
                  stdErr += mError->getErrText();
                }

                stdErr += "\n";
                retc = errno;
              }
            }

            if (mSubCmd == "get") {
              if (gOFS->_access(foundit->first.c_str(), R_OK, *mError, *pVid, 0)) {
                stdErr += "error: unable to get attributes of ";
                stdErr += foundit->first.c_str();
                retc = errno;
                return SFS_OK;
              }

              if (gOFS->_attr_get(foundit->first.c_str(), *mError, *pVid, (const char*) 0,
                                  key.c_str(), val)) {
                stdErr += "error: unable to get attribute ";
                stdErr += key;
                stdErr += " in file/directory ";
                stdErr += foundit->first.c_str();
                stdErr += "\n";
                retc = errno;
              } else {
                stdOut += key;
                stdOut += "=\"";
                stdOut += val;
                stdOut += "\"\n";
              }
            }

            if (mSubCmd == "rm") {
              if (gOFS->_attr_rem(foundit->first.c_str(), *mError, *pVid, (const char*) 0,
                                  key.c_str())) {
                stdErr += "error: unable to remove attribute '";
                stdErr += key;
                stdErr += "' in file/directory ";
                stdErr += foundit->first.c_str();
                stdErr += "\n";
                retc = errno;
              } else {
                stdOut += "success: removed attribute '";
                stdOut += key;
                stdOut += "' from file/directory ";
                stdOut += foundit->first.c_str();
                stdOut += "\n";
              }
            }

            if (mSubCmd == "fold") {
              int retc = gOFS->_attr_ls(foundit->first.c_str(), *mError, *pVid,
                                        (const char*) 0, map, true, false);

              if ((!retc) && map.count("sys.attr.link")) {
                retc |= gOFS->_attr_ls(map["sys.attr.link"].c_str(), *mError, *pVid,
                                       (const char*) 0, linkmap, true, true);
              }

              if (retc) {
                stdErr += "error: unable to list attributes in file/directory ";
                stdErr += foundit->first.c_str();
                stdErr += "\n";
                retc = errno;
              } else {
                XrdOucString partialStdOut;
                eos::IContainerMD::XAttrMap::const_iterator it;

                if (option == "r") {
                  stdOut += foundit->first.c_str();
                  stdOut += ":\n";
                }

                for (it = map.begin(); it != map.end(); ++it) {
                  if (linkmap.count(it->first) &&
                      linkmap[it->first] == map[it->first]) {
                    if (gOFS->_attr_rem(foundit->first.c_str(), *mError, *pVid, (const char*) 0,
                                        it->first.c_str())) {
                      stdErr += "error [ attr fold ] : unable to remove local attribute ";
                      stdErr += it->first.c_str();
                      stdErr += "\n";
                      retc = errno;
                    } else {
                      stdOut += "info [ attr fold ] : removing local attribute ";
                      stdOut += (it->first).c_str();
                      stdOut += "=";
                      stdOut += "\"";
                      stdOut += (it->second).c_str();
                      stdOut += "\"";
                      stdOut += "\n";
                    }
                  }
                }

                if (option == "r") {
                  stdOut += "\n";
                }
              }
            }
          }
        }
      }
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
