/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
// desc:   Namespace etag utilities
//------------------------------------------------------------------------------

#include <string>
#include "proto/FileMd.pb.h"

#ifndef EOS_NS_ETAG_HH
#define EOS_NS_ETAG_HH

namespace eos
{

  class IFileMD;
  class IContainerMD;

  //----------------------------------------------------------------------------
  //! Calculate etag for the given FileMD.
  //----------------------------------------------------------------------------
  void calculateEtag(const IFileMD *const fmd, std::string &out);
  void calculateEtag(const eos::ns::FileMdProto &proto, std::string &out);

  //----------------------------------------------------------------------------
  //! Calculate etag for the given ContainerMD.
  //! TODO(gbitzes): Make cmd const?
  //----------------------------------------------------------------------------
  void calculateEtag(IContainerMD *cmd, std::string &out);

}

#endif
