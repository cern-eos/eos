//------------------------------------------------------------------------------
//! @file ClayCode.hh
//! @brief Coupled-layer (Clay) erasure codec for the fixed EOS profiles
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
 *                                                                      *
 * Adapted from Ceph src/erasure-code/clay (LGPL-2.1-or-later):         *
 *   Copyright (C) 2018 Indian Institute of Science                     *
 *   Author: Myna Vajha <mynaramana@gmail.com>                          *
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
#include "fst/Namespace.hh"
#include <map>
#include <memory>
#include <set>
#include <vector>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Clay (coupled-layer) MDS codec.
//!
//! Two profiles are supported, both with a single-failure repair that reads
//! half of each helper stripe:
//!   - Clay10: 10 data + 2 parity, 12 stripes, 11 helpers
//!   - Clay15: 15 data + 3 parity, 18 stripes, 16 helpers
//------------------------------------------------------------------------------
class ClayCode
{
public:
  enum class Profile {
    Clay10,
    Clay15
  };

  // A contiguous run of sub-chunks inside one stripe block.
  struct Range {
    int index;
    int count;
  };

  explicit ClayCode(Profile profile);
  ~ClayCode();

  ClayCode(const ClayCode&) = delete;
  ClayCode& operator=(const ClayCode&) = delete;

  Profile GetProfile() const
  {
    return profile_;
  }
  int DataCount() const
  {
    return k_;
  }
  int ParityCount() const
  {
    return m_;
  }
  int StripeCount() const
  {
    return k_ + m_;
  }
  int HelperCount() const
  {
    return d_;
  }
  int SubChunkCount() const
  {
    return sub_chunk_no_;
  }
  int CouplingFactor() const
  {
    return q_;
  }

  //----------------------------------------------------------------------------
  //! Fill parity stripes. chunks has StripeCount() buffers of chunk_size
  //! bytes. The first DataCount() buffers are the payload.
  //! chunk_size must be a positive multiple of SubChunkCount().
  //!
  //! @return 0 on success, negative on error
  //----------------------------------------------------------------------------
  int Encode(char** chunks, int chunk_size);

  //----------------------------------------------------------------------------
  //! Reconstruct erased stripes in place. Every stripe buffer is present.
  //! Buffers listed in erased are outputs; the others are inputs.
  //!
  //! @return 0 on success, negative on error
  //----------------------------------------------------------------------------
  int Decode(const std::set<int>& erased, char** chunks, int chunk_size);

  //----------------------------------------------------------------------------
  //! Helpers contacted to repair one lost stripe. The lost index is absent.
  //----------------------------------------------------------------------------
  std::vector<int> RepairHelpers(int lost) const;

  //----------------------------------------------------------------------------
  //! Sub-chunk runs read from every helper. Pack helper bytes in this order.
  //----------------------------------------------------------------------------
  std::vector<Range> RepairRanges(int lost) const;

  //----------------------------------------------------------------------------
  //! Bytes read from each helper. This is chunk_size / CouplingFactor().
  //----------------------------------------------------------------------------
  int RepairBytes(int chunk_size) const;

  //----------------------------------------------------------------------------
  //! Rebuild one lost stripe from packed helper sub-chunks.
  //! helper_data[i] belongs to helpers[i] and holds RepairBytes() bytes.
  //! recovered receives a full chunk_size stripe.
  //!
  //! @return 0 on success, negative on error
  //----------------------------------------------------------------------------
  int Repair(int lost, const std::vector<int>& helpers, char** helper_data,
             char* recovered, int chunk_size);

private:
  struct ScalarMds;

  int Prepare(int chunk_size) const;
  int DecodeNodes(std::set<int>& erased, std::vector<char*>& nodes,
                  int chunk_size);
  int DecodeLayered(std::set<int>& erased, std::vector<char*>& nodes,
                    std::vector<std::vector<char>>& uncoupled, int chunk_size);
  int DecodeErasures(const std::set<int>& erased, int z,
                     std::vector<char*>& nodes,
                     std::vector<std::vector<char>>& uncoupled, int sc_size);
  int DecodeUncoupled(const std::set<int>& erased, int z,
                      std::vector<std::vector<char>>& uncoupled, int sc_size);
  void SetPlaneOrder(std::vector<int>& order, const std::set<int>& erased) const;
  int RecoverType1(std::vector<char*>& nodes, int x, int y, int z,
                   const int* z_vec, std::vector<std::vector<char>>& uncoupled,
                   int sc_size);
  int CoupledFromUncoupled(std::vector<char*>& nodes, int x, int y, int z,
                           const int* z_vec,
                           std::vector<std::vector<char>>& uncoupled,
                           int sc_size);
  int UncoupledFromCoupled(std::vector<char*>& nodes, int x, int y, int z,
                           const int* z_vec,
                           std::vector<std::vector<char>>& uncoupled,
                           int sc_size);
  int MaxIscore(const std::set<int>& erased) const;
  void PlaneVector(int z, int* z_vec) const;
  std::vector<Range> RepairRangesNode(int lost_node) const;
  int RepairSubchunkCount(int lost) const;
  int RepairOne(std::map<int, char*>& recovered, const std::set<int>& aloof,
                std::map<int, char*>& helpers, int repair_bytes,
                const std::vector<Range>& ranges,
                std::vector<std::vector<char>>& uncoupled);

  static int Pow(int base, int exp);

  Profile profile_;
  int k_;
  int m_;
  int d_;
  int q_;
  int t_;
  int nu_;
  int sub_chunk_no_;
  std::unique_ptr<ScalarMds> mds_;
  std::unique_ptr<ScalarMds> pft_;
};

EOSFSTNAMESPACE_END
