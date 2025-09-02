//------------------------------------------------------------------------------
//! @file FileIo.cc
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Abstract class modelling an IO plugin
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "fst/io/FileIo.hh"
#ifdef HAVE_NFS
#include "fst/io/nfs/NfsIo.hh"
#endif

EOSFSTNAMESPACE_BEGIN

//--------------------------------------------------------------------------
//! Rename operation
//--------------------------------------------------------------------------
int FileIo::fsRename(std::string old_path, std::string new_path)
{
  if ((old_path.find("nfs:/") == 0) || (new_path.find("nfs:/") == 0)) {
#ifdef HAVE_NFS
    return NfsIo::fsRename(old_path, new_path);
#endif
    eos_static_crit("%s", "msg=\"no NFS built-in support!\"");
    return ENOTSUP;
  }

  return ::rename(old_path.c_str(), new_path.c_str());
}

EOSFSTNAMESPACE_END
