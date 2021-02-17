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

//------------------------------------------------------------------------------
// author: Georgios Bitzes <georgios.bitzes@cern.ch>
// desc:   Namespace stat utilities
//------------------------------------------------------------------------------

#pragma once
#include "namespace/interface/IContainerMD.hh"

namespace eos
{
//----------------------------------------------------------------------------
//! Helper function for deriving the mode_t from a ContainerMD.
//----------------------------------------------------------------------------
inline mode_t modeFromMetadataEntry(const std::shared_ptr<eos::IContainerMD>&
                                    cmd)
{
  mode_t retval = cmd->getMode();

  if (cmd->numAttributes()) {
    retval |= S_ISVTX;
  }

  return retval;
}

//----------------------------------------------------------------------------
//! Helper function for deriving the mode_t from a FileMD.
//----------------------------------------------------------------------------
inline mode_t modeFromMetadataEntry(const std::shared_ptr<eos::IFileMD>& fmd)
{
  mode_t retval;

  // Symbolic link?
  if (fmd->isLink()) {
    retval = (S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO);
    return retval;
  }

  // Not a symbolic link
  retval = S_IFREG;
  uint16_t flags = fmd->getFlags();

  if (!flags) {
    retval |= (S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR);
  } else {
    retval |= flags;
  }

  if (fmd->hasLocation(EOS_TAPE_FSID)) {
    retval |= EOS_TAPE_MODE_T;
  }

  return retval;
}
}
