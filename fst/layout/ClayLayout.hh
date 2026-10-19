//------------------------------------------------------------------------------
//! @file ClayLayout.hh
//! @brief Clay erasure-coded RAIN layout
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#pragma once
#include "fst/layout/RainMetaLayout.hh"
#include "fst/layout/clay/ClayCode.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Clay layout. clay10 is 10+2 and clay15 is 15+3. Stripe geometry matches
//! Reed-Solomon. A single missing stripe is rebuilt from half of each helper.
//------------------------------------------------------------------------------
class ClayLayout : public RainMetaLayout
{
public:
  ClayLayout(XrdFstOfsFile* file, unsigned long lid,
             const XrdSecEntity* client, XrdOucErrInfo* outError,
             const char* path, eos::fst::FmdHandler* fmdHandler,
             uint16_t timeout = 0, bool storeRecovery = false,
             off_t targetSize = 0, std::string bookingOpaque = "oss.size",
             bool computeStripeChecksum = false);

  virtual ~ClayLayout() = default;

  virtual int Fallocate(XrdSfsFileOffset length) override;
  virtual int Fdeallocate(XrdSfsFileOffset fromOffset,
                          XrdSfsFileOffset toOffset) override;

private:
  ClayLayout& operator=(const ClayLayout&) = delete;
  ClayLayout(const ClayLayout&) = delete;
  ClayLayout& operator=(ClayLayout&&) = delete;
  ClayLayout(ClayLayout&&) = delete;

  bool ComputeParity(std::shared_ptr<eos::fst::RainGroup>& grp) override;
  int WriteParityToFiles(std::shared_ptr<eos::fst::RainGroup>& grp) override;
  bool RecoverPiecesInGroup(XrdCl::ChunkList& grp_errs) override;
  unsigned int MapSmallToBig(unsigned int idSmall) override;
  std::pair<int, uint64_t> GetLocalOff(uint64_t global_off) override;
  uint64_t GetGlobalOff(int stripe_id, uint64_t local_off) override;
  uint64_t GetStripeTruncateOffset(uint64_t offset) override;

  bool RepairOneStripe(eos::fst::RainGroup& blocks, unsigned int lost,
                       uint64_t offset_local, uint64_t offset_group,
                       XrdCl::ChunkList& grp_errs);
  bool DecodeGroup(std::shared_ptr<eos::fst::RainGroup>& grp,
                   uint64_t offset_local, uint64_t offset_group,
                   XrdCl::ChunkList& grp_errs);
  void PublishRecovered(eos::fst::RainGroup& blocks, unsigned int stripe_id,
                        uint64_t offset_group, XrdCl::ChunkList& grp_errs);
  bool StoreRecovered(eos::fst::RainGroup& blocks, unsigned int stripe_id,
                      uint64_t offset_local);

  ClayCode mCode;
};

EOSFSTNAMESPACE_END
