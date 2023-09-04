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

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
#include "common/Path.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Resolver.hh"
#include "namespace/Prefetcher.hh"

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
  bool exclusive = false;
  ACCESSMODE_R;
  NAMESPACEMAP;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;
  WAIT_BOOT;

  if ((spath.beginswith("fid:") || spath.beginswith("fxid:"))) {
    identifier = Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();
    spath = "";
    GetPathFromFid(spath, identifier, "error: ");
  } else if (spath.beginswith("pid:") || spath.beginswith("pxid:") ||
             spath.beginswith("cid:") || spath.beginswith("cxid:")) {
    if (spath.beginswith("pid:") || spath.beginswith("pxid:")) {
      spath.replace('p', 'f', 0, 1);
    } else {
      spath.replace('c', 'f', 0, 1);
    }

    identifier = Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();
    spath = "";
    GetPathFromCid(spath, identifier, "error: ");
  } else {
    spath = eos::common::Path(path).GetPath();
    eos::common::StringConversion::UnsealXrdPath(spath);
  }

  path = spath.c_str();
  PROC_TOKEN_SCOPE;

  if ((!spath.length()) && (!identifier)) {
    // Empty path or invalid numeric identifier
    stdErr = "error: please give a valid identifier (<path>|fid:<fid-dec>"
             "|fxid:<fid-hex>|cid:<cid-dec>|cxid:<cid-hex>)";
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
      bool isRecursive = false;
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
      using foundmap = std::map<std::string, std::set<std::string> >;
      using foundmapptr = std::unique_ptr<foundmap>;

      std::vector<foundmapptr> toBrowse;
      foundmapptr found = std::make_unique<foundmap>();
      (*found)[spath.c_str()].size();
      toBrowse.push_back(std::move(found));
      if (option.find("r") != STR_NPOS) {
        isRecursive = true;
      }

      if (option.find("c") != STR_NPOS) {
        exclusive = true;
      }

      if ((mSubCmd == "set") || (mSubCmd == "rm")) {
        SET_ACCESSMODE_W;
      }

      if (!retc) {
        // apply to  directories starting at the highest level
        uint64_t cumulLockTime = 0;
        uint64_t cumulFindTime = 0;
        uint64_t cumulAccessTime = 0;
        uint64_t cumulAttrLsTime = 0;
        uint64_t cumulInnerLoopTime = 0;
        uint64_t cumulOuterLoopTime = 0;
        uint64_t cumulEosViewGet = 0;
        uint64_t cumulContLock = 0;
        auto startOuterLoop = std::chrono::steady_clock::now();
        for(size_t i = 0; i < toBrowse.size(); ++i) {
          auto start = std::chrono::steady_clock::now();
          for(auto foundit = toBrowse[i]->begin(); foundit != toBrowse[i]->end(); foundit++) {
            std::unique_ptr<eos::IContainerMD::IContainerMDWriteLocker> contLock;
            std::unique_ptr<eos::IFileMD::IFileMDWriteLocker> fileLock;
            eos::IFileMDPtr file;
            eos::IContainerMDPtr cont;
            eos::IContainerMD::XAttrMap map;
            eos::IContainerMD::XAttrMap linkmap;
            {
              auto start = std::chrono::steady_clock::now();
              try {
                auto startViewGet = std::chrono::steady_clock::now();
                cont = gOFS->eosView->getContainer(foundit->first);
                eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView, foundit->first,true,true);
                auto endViewGet = std::chrono::steady_clock::now();
                cumulEosViewGet += chrono::duration_cast<std::chrono::nanoseconds>(endViewGet - startViewGet).count();
                auto startContLock = std::chrono::steady_clock::now();
                contLock = std::make_unique<eos::IContainerMD::IContainerMDWriteLocker>(cont);
                auto endContLock = std::chrono::steady_clock::now();
                cumulContLock += chrono::duration_cast<std::chrono::nanoseconds>(endContLock - startContLock).count();
              } catch(const eos::MDException &) {
                //Not a directory, it's a file
                try {
                  file = gOFS->eosView->getFile(foundit->first);
                  fileLock = std::make_unique<eos::IFileMD::IFileMDWriteLocker>(file);
                } catch (const eos::MDException &) {
                    stdErr += "error: unable to search in path ";
                    stdErr += toBrowse[i]->begin()->first.c_str();
                    retc = errno;
                }
              }
              auto end = std::chrono::steady_clock::now();
              cumulLockTime += chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            }
            if(isRecursive && !fileLock) {
              auto start = std::chrono::steady_clock::now();
              found = std::make_unique<foundmap>();
              if (gOFS->_find(foundit->first.c_str(), *mError, stdErr, *pVid, *found, nullptr,
                              nullptr, true,0, true, 1, nullptr, true)) {
                stdErr += "error: unable to search in path";
                retc = errno;
              }
              auto end = std::chrono::steady_clock::now();
              cumulFindTime += chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
              if(found->size()) {
                found->erase(foundit->first.back() != '/' ?  foundit->first + "/" : foundit->first);
                toBrowse.push_back(std::move(found));
              }
            }

            if ((mSubCmd == "ls")) {
              RECURSIVE_STALL("AttrLs", (*pVid));
              {
                auto start = std::chrono::steady_clock::now();
                if (gOFS->_access(foundit->first.c_str(), R_OK, *mError, *pVid,
                                  0, false)) {
                  stdErr += "error: unable to get attributes  ";
                  stdErr += foundit->first.c_str();
                  retc = errno;
                  return SFS_OK;
                }
                auto end = std::chrono::steady_clock::now();
                cumulAccessTime += chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
              }
              XrdOucString partialStdOut = "";
              auto start = std::chrono::steady_clock::now();
              if (gOFS->_attr_ls(foundit->first.c_str(), *mError, *pVid, (const char*) 0, map,
                                 false, true)) {
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
              auto end = std::chrono::steady_clock::now();
              cumulAttrLsTime += chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            }

            if (mSubCmd == "set") {
              RECURSIVE_STALL("AttrSet", (*pVid));

              if (key == "user.acl") {
                XrdOucString evalacl;

                // If someone wants to set a user.acl and the tag sys.eval.useracl
                // is not there, we return an error ...
                if ((pVid->uid != 0) && gOFS->_attr_get(foundit->first.c_str(),
                                                        *mError, *pVid,
                                                        (const char*) 0,
                                                        "sys.eval.useracl", evalacl,false)) {
                  stdErr += "error: unable to set user.acl - the file/directory does not "
                            "evaluate user acls (sys.eval.useracl is undefined)!\n";
                  retc = EINVAL;
                  return SFS_OK;
                }
              }

              // Check if the origin exists and is a directory
              if (key == "sys.attr.link") {

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
                                  key.c_str(), val.c_str(), false, exclusive)) {
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
              RECURSIVE_STALL("AttrGet", (*pVid));

              if (gOFS->_access(foundit->first.c_str(), R_OK, *mError, *pVid, 0,false)) {
                stdErr += "error: unable to get attributes of ";
                stdErr += foundit->first.c_str();
                retc = errno;
                return SFS_OK;
              }

              if (gOFS->_attr_get(foundit->first.c_str(), *mError, *pVid, (const char*) 0,
                                  key.c_str(), val,false)) {
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
              RECURSIVE_STALL("AttrRm", (*pVid));

              if (gOFS->_attr_rem(foundit->first.c_str(), *mError, *pVid, (const char*) 0,
                                  key.c_str(),false)) {
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
              RECURSIVE_STALL("AttrLs", (*pVid));
              int retc = gOFS->_attr_ls(foundit->first.c_str(), *mError, *pVid,
                                        (const char*) 0, map, false, false);

              if ((!retc) && map.count("sys.attr.link")) {
                retc |= gOFS->_attr_ls(map["sys.attr.link"].c_str(), *mError, *pVid,
                                       (const char*) 0, linkmap, false, true);
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
                                        it->first.c_str(),false)) {
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
          auto stop = std::chrono::steady_clock::now();
          cumulInnerLoopTime += std::chrono::duration_cast<std::chrono::nanoseconds >(stop - start).count();
        }
        auto stopOuterLoop = std::chrono::steady_clock::now();
        cumulOuterLoopTime += std::chrono::duration_cast<std::chrono::nanoseconds >(stopOuterLoop - startOuterLoop).count();
        eos_static_crit("cumulLockTime = %ld, cumulFindTime = %ld, cumulAccessTime = %ld, cumulAttrLsTime = %ld, cumulInnerLoopTime = %ld, cumulOuterLoopTime = %ld, cumulEosViewGet = %ld, cumulContLock = %ld\n",cumulLockTime, cumulFindTime, cumulAccessTime,cumulAttrLsTime, cumulInnerLoopTime, cumulOuterLoopTime, cumulEosViewGet, cumulContLock);
      }
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
