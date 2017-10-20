//------------------------------------------------------------------------------
//! @file data.cc
//! @author Andreas-Joachim Peters CERN
//! @brief class keeping dir contents at a given level
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "data/dircleaner.hh"
#undef __USE_FILE_OFFSET64
#include <fts.h>
#define __USE_FILE_OFFSET64

/* -------------------------------------------------------------------------- */
dircleaner::dircleaner(const std::string _path,
                       int64_t _maxsize,
                       int64_t _maxfiles) : path(_path),
max_files(_maxfiles),
max_size(_maxsize)

/* -------------------------------------------------------------------------- */
{
  if (max_files | max_size)
  {
    tLeveler = std::thread(&dircleaner::leveler, this);
  }
}

/* -------------------------------------------------------------------------- */
dircleaner::~dircleaner()
/* -------------------------------------------------------------------------- */
{
  if (max_files | max_size)
  {
    // C++11 is poor for threading
    pthread_cancel(tLeveler.native_handle());
    tLeveler.join();
  }
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
dircleaner::has_suffix(const std::string& str, const std::string &suffix)
/* -------------------------------------------------------------------------- */
{
  return str.size() >= suffix.size() &&
          str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
dircleaner::cleanall(std::string matchsuffix)
/* -------------------------------------------------------------------------- */
{
  std::lock_guard<std::recursive_mutex> mLock(cleaningMutex);

  if (!scanall(matchsuffix))
  {
    std::string tout;
    treeinfo.Print(tout);
    eos_static_info("purging %s", tout.c_str());
    for (auto it = treeinfo.treemap.begin(); it != treeinfo.treemap.end(); ++it)
    {
      if (matchsuffix.length() && (!has_suffix(it->second.path, matchsuffix)))
      {
	continue;
      }
      if (unlink(it->second.path.c_str()) && errno != ENOENT)
      {
	
	eos_static_err("unlink: path=%s errno=%d", it->second.path.c_str(), errno);
      }
    }
  }
  return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
dircleaner::tree_info::Print(std::string& out)
/* -------------------------------------------------------------------------- */
{

  char line[1024];
  snprintf(line, sizeof (line), "path=%s n-files=%lu tree-size=%lu",
           path.c_str(),
           totalfiles,
           totalsize);
  out += line;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
dircleaner::scanall(std::string matchsuffix)
/* -------------------------------------------------------------------------- */
{
  int retc = 0;
  char *paths[2];
  paths[0] = (char*) path.c_str();
  paths[1] = 0;

  FTS *tree = fts_open(paths, FTS_NOCHDIR, 0);
  if (!tree)
  {
    return -1; // see errno
  }
  FTSENT *node;

  treeinfo.path = path;
  treeinfo.totalfiles = 0;
  treeinfo.totalsize = 0;
  treeinfo.treemap.clear();
  externaltreeinfo.reset();

  while ((node = fts_read(tree)))
  {
    // skip any hidden file, we might want that
    if (node->fts_level > 0 && node->fts_name[0] == '.')
    {
      fts_set(tree, node, FTS_SKIP);
    }
    else
    {
      if (node->fts_info == FTS_F)
      {
        std::string filepath = node->fts_accpath;

	if (matchsuffix.length() && !has_suffix(filepath, matchsuffix))
	{
	  continue;
	}

        struct stat buf;
        if (::stat(filepath.c_str(), &buf))
        {
          if ( errno == ENOENT)
          {
            // this can happen when something get's cleaned during scanning
            eos_static_info("stat: path=%s errno=%d", filepath.c_str(), errno);
          }
          else
          {
            eos_static_err("stat: path=%s errno=%d", filepath.c_str(), errno);
            retc = -1;
          }
        }
        else
        {
          treeinfo.totalfiles++;
          treeinfo.totalsize += buf.st_size;
          file_info_t finfo;
          finfo.path = filepath;
          finfo.mtime = buf.st_mtime;
          finfo.size = buf.st_size;
          treeinfo.treemap.insert(std::pair<time_t, file_info_t>(finfo.mtime, finfo));
          eos_static_debug("adding path=%s mtime=%lu size=%lu", filepath.c_str(),
                           buf.st_mtime,
                           buf.st_size);
        }
      }
    }
  }

  if (fts_close(tree))
  {
    eos_static_err("fts_close: errno=%d", errno);
    return -1;
  }
  return retc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
dircleaner::trim(bool force)
{
  if (!force)
  {
    // avoid full scans
    tree_info_t& externaltree = get_external_tree();
    bool size_ok = true;
    bool files_ok = true;

    // an external cache can give change hints via the externaltree
    int64_t tree_size = treeinfo.totalsize + externaltree.get_size();
    int64_t tree_files = treeinfo.totalfiles + externaltree.get_files();

    eos_static_info("max-size=%ld is-size=%ld max-files=%lld is-files=%ld force=%d",
                    max_size, tree_size,
                    max_files, tree_files,
                    force);

    if (max_size && (tree_size > max_size))
    {
      size_ok = false;
    }

    if (max_files && (tree_files > max_files))
    {
      files_ok = false;
    }

    // nothing to do
    if (size_ok && files_ok)
      return 0;
  }

  scanall(trim_suffix);

  for (auto it = treeinfo.treemap.begin(); it != treeinfo.treemap.end(); ++it)
  {
    eos_static_debug("is-size %lld max-size %lld", treeinfo.totalsize, max_size);
    bool size_ok = true;
    bool files_ok = true;

    if (max_size && (treeinfo.totalsize > max_size))
    {
      size_ok = false;
    }

    if (max_files && (treeinfo.totalfiles > max_files))
    {
      files_ok = false;
    }

    // nothing to do
    if (size_ok && files_ok)
      return 0;

    eos_static_info("erasing %s %ld => %ld", it->second.path.c_str(), treeinfo.totalsize, it->second.size);
    if (::unlink(it->second.path.c_str()))
    {
      eos_static_err("failed to unlink file %s errno=%d", it->second.path.c_str(), errno);
    }
    else
    {
      treeinfo.totalfiles--;
      treeinfo.totalsize-=it->second.size;
    }
  }
  return 0;
}
/* -------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
dircleaner::leveler()
/* -------------------------------------------------------------------------- */
{
  size_t n=0;
  while (1)
  {
    XrdSysTimer sleeper;
    sleeper.Snooze(15);
    std::lock_guard<std::recursive_mutex> mLock(cleaningMutex);
    trim(!n % (1 * 60 * 4)); // forced trim every hour
    n++;
  }

}
