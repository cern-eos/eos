//------------------------------------------------------------------------------
//! @file NsCmd.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include "mgm/Namespace.hh"
#include "common/Ns.pb.h"
#include "mgm/proc/ProcCommand.hh"
#include "namespace/interface/IContainerMD.hh"
#include "common/ConsoleRequest.pb.h"
#include <list>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class NsCmd - class hadling ns commands
//------------------------------------------------------------------------------
class NsCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit NsCmd(eos::console::RequestProto&& req,
                 eos::common::Mapping::VirtualIdentity& vid):
    IProcCommand(vid, false), mReqProto(std::move(req))
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~NsCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behvior of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() override;

private:
  eos::console::RequestProto mReqProto; ///< Client request protobuf object

  //----------------------------------------------------------------------------
  //! Execute mutex subcommand
  //!
  //! @param mutex mutex subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void MutexSubcmd(const eos::console::NsProto_MutexProto& mutex,
                   eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute stat comand
  //!
  //! @param stat stat subcommand proto object
  //!
  //! @return string representing stat output
  //----------------------------------------------------------------------------
  std::string StatSubcmd(const eos::console::NsProto_StatProto& stat);

  //----------------------------------------------------------------------------
  //! Execute master comand
  //!
  //! @param master master subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void MasterSubcmd(const eos::console::NsProto_MasterProto& master,
                    eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute compact comand
  //!
  //! @param compact compact subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void CompactSubcmd(const eos::console::NsProto_CompactProto& master,
                     eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Execute tree size recompute
  //!
  //! @param tree tree size  subcommand proto object
  //! @param reply reply proto object
  //----------------------------------------------------------------------------
  void TreeSizeSubcmd(const eos::console::NsProto_TreeSizeProto& tree,
                      eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Do a breadth first search of all the subcontainers under the given
  //! container
  //!
  //! @param cont container object
  //! @param max_depth maxinum depth scanned for updating the tree size, 0
  //!        means no limit
  //!
  //! @return list containing lists of subcontainers at each depth level
  //!         starting with level 0 in front represenint the given container
  //! @note this function assumes a write lock on eosViewRWMutex
  //----------------------------------------------------------------------------
  std::list< std::list<eos::IContainerMD::id_t> >
  BreadthFirstSearchContainers(eos::IContainerMD* cont,
                               uint32_t max_depth = 0) const;

  //----------------------------------------------------------------------------
  //! Recompute and update tree size of the given container assumming its
  //! subcontainers tree size values are correct and adding the size of files
  //! attached directly to the current container
  //!
  //! @param cont container object
  //----------------------------------------------------------------------------
  void UpdateTreeSize(eos::IContainerMD* cont) const;
};

EOSMGMNAMESPACE_END
