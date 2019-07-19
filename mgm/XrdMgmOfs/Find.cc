// ----------------------------------------------------------------------
// File: Find.cc
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

// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

//------------------------------------------------------------------------------
// Low-level namespace find command
//------------------------------------------------------------------------------
int
XrdMgmOfs::_find(const char* path, XrdOucErrInfo& out_error,
                 XrdOucString& stdErr, eos::common::VirtualIdentity& vid,
                 std::map<std::string, std::set<std::string> >& found,
                 const char* key, const char* val, bool no_files,
                 time_t millisleep, bool nscounter, int maxdepth,
                 const char* filematch, bool take_lock)
{
  std::vector< std::vector<std::string> > found_dirs;
  std::shared_ptr<eos::IContainerMD> cmd;
  std::string Path = path;
  EXEC_TIMING_BEGIN("Find");

  if (nscounter) {
    gOFS->MgmStats.Add("Find", vid.uid, vid.gid, 1);
  }

  if (*Path.rbegin() != '/') {
    Path += '/';
  }

  errno = 0;
  found_dirs.resize(1);
  found_dirs[0].resize(1);
  found_dirs[0][0] = Path.c_str();
  int deepness = 0;
  // Users cannot return more than 100k files and 50k dirs with one find,
  // unless there is an access rule allowing deeper queries
  static uint64_t dir_limit = 50000;
  static uint64_t file_limit = 100000;
  Access::GetFindLimits(vid, dir_limit, file_limit);
  uint64_t filesfound = 0;
  uint64_t dirsfound = 0;
  bool limitresult = false;
  bool limited = false;

  if ((vid.uid != 0) && (!vid.hasUid(3)) && (!vid.hasGid(4)) && (!vid.sudoer)) {
    limitresult = true;
  }

  do {
    bool permok = false;
    found_dirs.resize(deepness + 2);

    // Loop over all directories in that deepness
    for (unsigned int i = 0; i < found_dirs[deepness].size(); i++) {
      Path = found_dirs[deepness][i].c_str();
      eos_static_debug("Listing files in directory %s", Path.c_str());

      // Slow down the find command without holding locks
      if (millisleep) {
        std::this_thread::sleep_for(std::chrono::milliseconds(millisleep));
      }

      // Held only for the current loop
      eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView,
          Path.c_str());
      eos::common::RWMutexReadLock ns_rd_lock;

      if (take_lock) {
        ns_rd_lock.Grab(gOFS->eosViewRWMutex);
      }

      try {
        cmd = gOFS->eosView->getContainer(Path.c_str(), false);
        permok = cmd->access(vid.uid, vid.gid, R_OK | X_OK);
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        cmd.reset();
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
      }

      if (!gOFS->allow_public_access(Path.c_str(), vid)) {
        stdErr += "error: public access level restriction - no access in  ";
        stdErr += Path.c_str();
        stdErr += "\n";
        continue;
      }

      if (cmd) {
        if (!permok) {
          // check-out for ACLs
          permok = _access(Path.c_str(), R_OK | X_OK, out_error, vid, "",
                           false) ? false : true;
        }

        if (!permok) {
          stdErr += "error: no permissions to read directory ";
          stdErr += Path.c_str();
          stdErr += "\n";
          continue;
        }

        // Add all children into the 2D vectors
        for (auto dit = eos::ContainerMapIterator(cmd); dit.valid(); dit.next()) {
          std::string fpath = Path.c_str();
          fpath += dit.key();
          fpath += "/";

          // check if we select by tag
          if (key) {
            XrdOucString wkey = key;

            if (wkey.find("*") != STR_NPOS) {
              // this is a search for 'beginswith' match
              eos::IContainerMD::XAttrMap attrmap;

              if (!gOFS->_attr_ls(fpath.c_str(), out_error, vid,
                                  (const char*) 0, attrmap, !take_lock)) {
                for (auto it = attrmap.begin(); it != attrmap.end(); it++) {
                  XrdOucString akey = it->first.c_str();

                  if (akey.matches(wkey.c_str())) {
                    // Trick to add element
                    (void)found[fpath].size();
                  }
                }
              }

              found_dirs[deepness + 1].push_back(fpath.c_str());
            } else {
              // This is a search for a full match or a key search
              std::string sval = val;
              XrdOucString attr = "";

              if (!gOFS->_attr_get(fpath.c_str(), out_error, vid,
                                   (const char*) 0, key, attr, !take_lock)) {
                found_dirs[deepness + 1].push_back(fpath.c_str());

                if ((val == std::string("*")) || (attr == val)) {
                  (void)found[fpath].size();
                }
              }
            }
          } else {
            if (limitresult) {
              // Apply  user limits for non root/admin/sudoers
              if (dirsfound >= dir_limit) {
                stdErr += "warning: find results are limited for you to ndirs=";
                stdErr += (int) dir_limit;
                stdErr += " -  result is truncated!\n";
                limited = true;
                break;
              }
            }

            found_dirs[deepness + 1].push_back(fpath.c_str());
            (void)found[fpath].size();
            dirsfound++;
          }
        }

        if (!no_files) {
          std::string link;
          std::string fname;
          std::shared_ptr<eos::IFileMD> fmd;

          for (auto fit = eos::FileMapIterator(cmd); fit.valid(); fit.next()) {
            fname = fit.key();

            try {
              fmd = cmd->findFile(fname);
            } catch (eos::MDException& e) {
              fmd = 0;
            }

            if (fmd) {
              // Skip symbolic links
              if (fmd->isLink()) {
                link = fmd->getLink();
              } else {
                link.clear();
              }

              if (limitresult) {
                // Apply user limits for non root/admin/sudoers
                if (filesfound >= file_limit) {
                  stdErr += "warning: find results are limited for you to nfiles=";
                  stdErr += (int) file_limit;
                  stdErr += " -  result is truncated!\n";
                  limited = true;
                  break;
                }
              }

              if (!filematch) {
                if (link.length()) {
                  std::string ip = fname;
                  ip += " -> ";
                  ip += link;
                  found[Path].insert(ip);
                } else {
                  found[Path].insert(fname);
                }

                filesfound++;
              } else {
                XrdOucString name = fname.c_str();

                if (name.matches(filematch)) {
                  found[Path].insert(fname);
                  filesfound++;
                }
              }
            }
          }
        }
      }

      if (limited) {
        break;
      }
    }

    deepness++;

    if (limited) {
      break;
    }
  } while (found_dirs[deepness].size() && ((!maxdepth) || (deepness < maxdepth)));

  if (!no_files) {
    // If the result is empty, maybe this was a find by file
    if (!found.size()) {
      XrdSfsFileExistence file_exists;

      if (((_exists(Path.c_str(), file_exists, out_error, vid,
                    0, take_lock)) == SFS_OK) &&
          (file_exists == XrdSfsFileExistIsFile)) {
        eos::common::Path cPath(Path.c_str());
        found[cPath.GetParentPath()].insert(cPath.GetName());
      }
    }
  }

  // Include also the directory which was specified in the query if it is
  // accessible and a directory since it can evt. be missing if it is empty
  XrdSfsFileExistence dir_exists;

  if (((_exists(found_dirs[0][0].c_str(), dir_exists, out_error, vid,
                0, take_lock)) == SFS_OK)
      && (dir_exists == XrdSfsFileExistIsDirectory)) {
    eos::common::Path cPath(found_dirs[0][0].c_str());
    (void) found[found_dirs[0][0].c_str()].size();
  }

  if (nscounter) {
    EXEC_TIMING_END("Find");
  }

  return SFS_OK;
}
