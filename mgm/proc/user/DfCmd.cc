//------------------------------------------------------------------------------
//! @file DfCmd.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "DfCmd.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/config/IConfigEngine.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command
//------------------------------------------------------------------------------
eos::console::ReplyProto
DfCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::DfProto df = mReqProto.df();

  reply.set_std_out(FsView::gFsView.Df(df.monitoring(),df.si(),df.readable(), df.path(), WantsJsonOutput()));
  reply.set_retc(0);

  return reply;
}

EOSMGMNAMESPACE_END
