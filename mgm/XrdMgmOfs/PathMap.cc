// ----------------------------------------------------------------------
// File: PathMap.cc
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

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::ResetPathMap ()
/*----------------------------------------------------------------------------*/
/*
 * Reset all the stored entries in the path remapping table
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexWriteLock lock(PathMapMutex);
  PathMap.clear();
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::AddPathMap (const char* source,
                       const char* target)
/*----------------------------------------------------------------------------*/
/*
 * Add a source/target pair to the path remapping table
 *
 * @param source prefix path to map
 * @param target target path for substitution of prefix
 *
 * This function allows e.g. to map paths like /store/ to /eos/instance/store/
 * to provide an unprefixed global namespace in a storage federation.
 * It is used by the Configuration Engin to apply a mapping from a configuration
 * file.
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexWriteLock lock(PathMapMutex);
  if (PathMap.count(source))
  {
    return false;
  }
  else
  {
    PathMap[source] = target;
    ConfEngine->SetConfigValue("map", source, target);
    return true;
  }
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::PathRemap (const char* inpath,
                      XrdOucString &outpath)
/*----------------------------------------------------------------------------*/
/*
 * @brief translate a path name according to the configured mapping table
 *
 * @param inpath path to map
 * @param outpath remapped path
 *
 * This function does the path translation according to the configured mapping
 * table. It applies the 'longest' matching rule e.g. a rule
 * /eos/instance/store/ => /store/
 * would win over
 * /eos/instance/ = /global/
 * if the given path matches both prefixed like '/eos/instance/store/a'
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Path cPath(inpath);

  eos::common::RWMutexReadLock lock(PathMapMutex);
  eos_debug("mappath=%s ndir=%d dirlevel=%d", inpath, PathMap.size(), cPath.GetSubPathSize() - 1);

  outpath = inpath;

  // remove double slashes
  while (outpath.replace("//", "/"))
  {
  }

  // append a / to the path
  outpath += "/";

  if (!PathMap.size())
  {
    outpath.erase(outpath.length() - 1);
    return;
  }

  if (PathMap.count(inpath))
  {
    outpath.replace(inpath, PathMap[inpath].c_str());
    outpath.erase(outpath.length() - 1);
    return;
  }

  if (PathMap.count(outpath.c_str()))
  {
    outpath.replace(outpath.c_str(), PathMap[outpath.c_str()].c_str());
    outpath.erase(outpath.length() - 1);
    return;
  }

  if (!cPath.GetSubPathSize())
  {
    outpath.erase(outpath.length() - 1);
    return;
  }

  for (size_t i = cPath.GetSubPathSize() - 1; i > 0; i--)
  {
    if (PathMap.count(cPath.GetSubPath(i)))
    {
      outpath.replace(cPath.GetSubPath(i), PathMap[cPath.GetSubPath(i)].c_str(), 0, strlen(cPath.GetSubPath(i)));
      outpath.erase(outpath.length() - 1);
      return;
    }
  }
  outpath.erase(outpath.length() - 1);
  return;
}

