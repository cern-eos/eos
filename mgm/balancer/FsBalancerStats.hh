//------------------------------------------------------------------------------
//! @file FsBalancerStats.hh
//! @author Elvin Sindrilaru <esindril@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
o *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class FsView;
//! Helper alias
using BalancePair = std::pair<FsBalanceInfo, FsBalanceInfo>;

//------------------------------------------------------------------------------
//! Class FsBalancerStats is responsible for collecting and computing
//! statistics based on which the FsBalancer will decide what actions to take.
//------------------------------------------------------------------------------
class FsBalancerStats
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FsBalancerStats(const std::string& space_name):
    mSpaceName(space_name)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FsBalancerStats() = default;

  //----------------------------------------------------------------------------
  //! Update statistics with information from the group and file systems stats
  //!
  //! @param fs_view FsView aggregating all the fs/group objects
  //! @param threshold value for selecting file systems that deviate more than
  //!        this from the average values.
  //----------------------------------------------------------------------------
  void Update(FsView* fs_view, double threshold);

  //----------------------------------------------------------------------------
  //! Decide if an update of the data structures is needed
  //!
  //! @return true if update should be done, otherwise false
  //----------------------------------------------------------------------------
  bool NeedsUpdate();

  //----------------------------------------------------------------------------
  //! Get list of balance source and destination file systems to be used for
  //! doing transfers.
  //!
  //! @return list of source and destination file systems
  //----------------------------------------------------------------------------
  std::pair<std::set<FsBalanceInfo>, std::set<FsBalanceInfo>>
      GetTxEndpoints();

  //----------------------------------------------------------------------------
  //! Account for finished transfer by freeing up a slot
  //!
  //! @param src_node node identifier <host>:<port> for source
  //! @param dst_node node identifier <host>:<port> for destination
  //----------------------------------------------------------------------------
  void FreeTxSlot(const std::string& src_node, const std::string& dst_node);

  //----------------------------------------------------------------------------
  //! Check if node still has avilable transfer slots
  //!
  //! @param node_id node identifier <host>:<port>
  //! @param tx_per_node max number of transfers per node
  //!
  //! @return true if slot available, otherwise false
  //----------------------------------------------------------------------------
  bool HasTxSlot(const std::string& node_id, unsigned int tx_per_node) const;

#ifdef IN_TEST_HARNESS
public:
#else
private:
#endif

  //----------------------------------------------------------------------------
  //! Account for new transfer by reserving a slot
  //!
  //! @param src_node node identifier <host>:<port> for source
  //! @param dst_node node identifier <host>:<port> for destination
  //----------------------------------------------------------------------------
  void TakeTxSlot(const std::string& src_node, const std::string& dst_node);

  std::string mSpaceName;
  //! Map groups to balance (above threshold) to max deviation acting as a cache
  std::map<std::string, double> mGrpToMaxDev;
  //! Map groups to tuple of priority sets to be used when selecting source
  //! and destination filesystems
  std::map<std::string, FsPrioritySets> mGrpToPrioritySets;
  //! Map node FQDN to number of ongoing transfers
  std::map<std::string, unsigned int> mNodeNumTx;
  mutable std::mutex mMutex;
  std::chrono::seconds mUpdateInterval {60};
};

EOSMGMNAMESPACE_END
