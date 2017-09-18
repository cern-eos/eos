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
dircleaner::dircleaner()
/* -------------------------------------------------------------------------- */
{

}

/* -------------------------------------------------------------------------- */
dircleaner::~dircleaner()
/* -------------------------------------------------------------------------- */
{

}

int
dircleaner::cleanall(const std::string path)
{
  tree_info_t treeinfo;

  if (!scanall(path, treeinfo))
  {
    std::string tout;
    treeinfo.Print(tout);
    eos_static_info("purging %s", tout.c_str());
    for (auto it = treeinfo.treemap.begin(); it != treeinfo.treemap.end(); ++it)
    {
      if (unlink(it->second.path.c_str()) && errno != ENOENT)
      {
        eos_static_err("unlink: path=%s errno=%d", it->second.path.c_str(), errno);
      }
    }
  }
  return 0;
}

void dircleaner::tree_info::Print(std::string& out)
{
  char line[1024];
  snprintf(line, sizeof (line), "path=%s n-files=%lu tree-size=%lu",
           path.c_str(),
           totalfiles,
           totalsize);
  out += line;
}

int
dircleaner::scanall(const std::string path, dircleaner::tree_info_t& treeinfo)
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
          eos_static_debug("adding path=%s mtime=lu size=%lu", filepath.c_str(), 
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