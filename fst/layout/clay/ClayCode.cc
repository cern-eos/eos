//------------------------------------------------------------------------------
//! @file ClayCode.cc
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

#include "fst/layout/clay/ClayCode.hh"
#include "fst/layout/jerasure/include/jerasure.h"
#include "fst/layout/jerasure/include/reed_sol.h"
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <utility>

EOSFSTNAMESPACE_BEGIN

namespace
{

//------------------------------------------------------------------------------
//! Vandermonde Reed-Solomon used as Clay's scalar MDS and pairwise code.
//------------------------------------------------------------------------------
struct ScalarReedSol {
  int k;
  int m;
  int w;
  int* matrix;

  ScalarReedSol(int data, int parity)
    : k(data), m(parity), w(8), matrix(nullptr)
  {
    static std::mutex init_mu;
    std::lock_guard<std::mutex> lock(init_mu);
    matrix = reed_sol_vandermonde_coding_matrix(k, m, w);

    if (matrix == nullptr) {
      throw std::runtime_error("clay scalar Reed-Solomon init failed");
    }
  }

  ~ScalarReedSol()
  {
    free(matrix);
  }

  ScalarReedSol(const ScalarReedSol&) = delete;
  ScalarReedSol& operator=(const ScalarReedSol&) = delete;

  // known selects the inputs. decoded holds every shard pointer. Erased
  // shards are the decoded entries that are absent from known, and jerasure
  // writes them in place.
  int Decode(const std::map<int, char*>& known,
             const std::map<int, char*>& decoded, int size) const
  {
    std::vector<char*> data(k, nullptr);
    std::vector<char*> coding(m, nullptr);
    std::vector<int> erasures;
    erasures.reserve(static_cast<size_t>(m) + 1);

    for (int i = 0; i < k + m; ++i) {
      auto out = decoded.find(i);

      if ((out == decoded.end()) || (out->second == nullptr)) {
        return -1;
      }

      if (i < k) {
        data[i] = out->second;
      } else {
        coding[i - k] = out->second;
      }

      if (known.find(i) == known.end()) {
        erasures.push_back(i);
      }
    }

    if (erasures.empty()) {
      return 0;
    }

    erasures.push_back(-1);
    return jerasure_matrix_decode(k, m, w, matrix, 1, erasures.data(),
                                  data.data(), coding.data(), size);
  }
};

}  // namespace

struct ClayCode::ScalarMds : ScalarReedSol {
  using ScalarReedSol::ScalarReedSol;
};

int
ClayCode::Pow(int base, int exp)
{
  int power = 1;

  while (exp > 0) {
    if (exp & 1) {
      power *= base;
    }

    exp /= 2;
    base *= base;
  }

  return power;
}

ClayCode::ClayCode(Profile profile)
  : profile_(profile), k_(0), m_(0), d_(0), q_(0), t_(0), nu_(0),
    sub_chunk_no_(0)
{
  // Clay15 is 15 data + 3 parity. d = 16 keeps the sub-chunk count a power
  // of two so it divides every EOS block size. d = 17 would split a block
  // into 729 sub-chunks.
  if (profile_ == Profile::Clay10) {
    k_ = 10;
    m_ = 2;
    d_ = 11;
  } else if (profile_ == Profile::Clay15) {
    k_ = 15;
    m_ = 3;
    d_ = 16;
  } else {
    throw std::runtime_error("unknown clay profile");
  }

  if ((d_ < k_ + 1) || (d_ > k_ + m_ - 1)) {
    throw std::runtime_error("clay helper count out of range");
  }

  q_ = d_ - k_ + 1;

  if ((k_ + m_) % q_) {
    nu_ = q_ - ((k_ + m_) % q_);
  } else {
    nu_ = 0;
  }

  if ((k_ + m_ + nu_) > 254) {
    throw std::runtime_error("clay profile exceeds the Reed-Solomon field");
  }

  t_ = (k_ + m_ + nu_) / q_;
  sub_chunk_no_ = Pow(q_, t_);

  if ((profile_ == Profile::Clay10) &&
      ((k_ != 10) || (m_ != 2) || (sub_chunk_no_ != 64))) {
    throw std::runtime_error("clay10 profile is not 10+2");
  }

  if ((profile_ == Profile::Clay15) &&
      ((k_ != 15) || (m_ != 3) || (sub_chunk_no_ != 512))) {
    throw std::runtime_error("clay15 profile is not 15+3");
  }

  mds_.reset(new ScalarMds(k_ + nu_, m_));
  pft_.reset(new ScalarMds(2, 2));
}

ClayCode::~ClayCode() = default;

int
ClayCode::Prepare(int chunk_size) const
{
  if ((chunk_size <= 0) || (sub_chunk_no_ <= 0) ||
      ((chunk_size % sub_chunk_no_) != 0)) {
    return -1;
  }

  const int sc = chunk_size / sub_chunk_no_;

  // gf-complete region multiply wants a multiple of the vector width.
  if ((sc % 16) != 0) {
    return -1;
  }

  return sc;
}

int
ClayCode::Encode(char** chunks, int chunk_size)
{
  if ((chunks == nullptr) || (Prepare(chunk_size) < 0)) {
    return -1;
  }

  std::set<int> erased;

  for (int i = k_; i < k_ + m_; ++i) {
    if (chunks[i] == nullptr) {
      return -1;
    }

    memset(chunks[i], 0, static_cast<size_t>(chunk_size));
    erased.insert(i);
  }

  for (int i = 0; i < k_; ++i) {
    if (chunks[i] == nullptr) {
      return -1;
    }
  }

  return Decode(erased, chunks, chunk_size);
}

int
ClayCode::Decode(const std::set<int>& erased, char** chunks, int chunk_size)
{
  if ((chunks == nullptr) || (Prepare(chunk_size) < 0)) {
    return -1;
  }

  if (static_cast<int>(erased.size()) > m_) {
    return -1;
  }

  const int nodes_n = q_ * t_;
  std::vector<char*> nodes(static_cast<size_t>(nodes_n), nullptr);
  std::vector<std::vector<char>> shortened;

  if (nu_ > 0) {
    shortened.assign(static_cast<size_t>(nu_),
                     std::vector<char>(static_cast<size_t>(chunk_size), 0));
  }

  std::set<int> erased_nodes;

  for (int logical = 0; logical < k_ + m_; ++logical) {
    if (chunks[logical] == nullptr) {
      return -1;
    }

    const int node = (logical < k_) ? logical : logical + nu_;
    nodes[static_cast<size_t>(node)] = chunks[logical];

    if (erased.count(logical) != 0) {
      erased_nodes.insert(node);
    }
  }

  for (int i = 0; i < nu_; ++i) {
    nodes[static_cast<size_t>(k_ + i)] = shortened[static_cast<size_t>(i)].data();
  }

  for (int node : erased_nodes) {
    memset(nodes[static_cast<size_t>(node)], 0, static_cast<size_t>(chunk_size));
  }

  return DecodeNodes(erased_nodes, nodes, chunk_size);
}

int
ClayCode::DecodeNodes(std::set<int>& erased, std::vector<char*>& nodes,
                      int chunk_size)
{
  std::vector<std::vector<char>> uncoupled(
    static_cast<size_t>(q_ * t_),
    std::vector<char>(static_cast<size_t>(chunk_size), 0));
  return DecodeLayered(erased, nodes, uncoupled, chunk_size);
}

int
ClayCode::DecodeLayered(std::set<int>& erased, std::vector<char*>& nodes,
                        std::vector<std::vector<char>>& uncoupled,
                        int chunk_size)
{
  int num_erasures = static_cast<int>(erased.size());

  if (num_erasures <= 0) {
    return -1;
  }

  const int sc_size = chunk_size / sub_chunk_no_;

  for (int i = k_ + nu_; (num_erasures < m_) && (i < q_ * t_); ++i) {
    if (erased.insert(i).second) {
      ++num_erasures;
    }
  }

  if (num_erasures != m_) {
    return -1;
  }

  const int max_iscore = MaxIscore(erased);
  std::vector<int> order(static_cast<size_t>(sub_chunk_no_), 0);
  std::vector<int> z_vec(static_cast<size_t>(t_), 0);
  SetPlaneOrder(order, erased);

  for (int iscore = 0; iscore <= max_iscore; ++iscore) {
    for (int z = 0; z < sub_chunk_no_; ++z) {
      if (order[static_cast<size_t>(z)] == iscore) {
        if (DecodeErasures(erased, z, nodes, uncoupled, sc_size) != 0) {
          return -1;
        }
      }
    }

    for (int z = 0; z < sub_chunk_no_; ++z) {
      if (order[static_cast<size_t>(z)] != iscore) {
        continue;
      }

      PlaneVector(z, z_vec.data());

      for (int node_xy : erased) {
        const int x = node_xy % q_;
        const int y = node_xy / q_;
        const int node_sw = y * q_ + z_vec[static_cast<size_t>(y)];

        if (z_vec[static_cast<size_t>(y)] != x) {
          if (erased.count(node_sw) == 0) {
            if (RecoverType1(nodes, x, y, z, z_vec.data(), uncoupled,
                             sc_size) != 0) {
              return -1;
            }
          } else if (z_vec[static_cast<size_t>(y)] < x) {
            if (CoupledFromUncoupled(nodes, x, y, z, z_vec.data(), uncoupled,
                                     sc_size) != 0) {
              return -1;
            }
          }
        } else {
          const size_t off = static_cast<size_t>(z) * static_cast<size_t>(sc_size);
          memcpy(nodes[static_cast<size_t>(node_xy)] + off,
                 uncoupled[static_cast<size_t>(node_xy)].data() + off,
                 static_cast<size_t>(sc_size));
        }
      }
    }
  }

  return 0;
}

int
ClayCode::DecodeErasures(const std::set<int>& erased, int z,
                         std::vector<char*>& nodes,
                         std::vector<std::vector<char>>& uncoupled, int sc_size)
{
  std::vector<int> z_vec(static_cast<size_t>(t_), 0);
  PlaneVector(z, z_vec.data());

  for (int x = 0; x < q_; ++x) {
    for (int y = 0; y < t_; ++y) {
      const int node_xy = q_ * y + x;
      const int node_sw = q_ * y + z_vec[static_cast<size_t>(y)];

      if (erased.count(node_xy) != 0) {
        continue;
      }

      const int zx = z_vec[static_cast<size_t>(y)];

      if (zx < x) {
        if (UncoupledFromCoupled(nodes, x, y, z, z_vec.data(), uncoupled,
                                 sc_size) != 0) {
          return -1;
        }
      } else if (zx == x) {
        const size_t off = static_cast<size_t>(z) * static_cast<size_t>(sc_size);
        memcpy(uncoupled[static_cast<size_t>(node_xy)].data() + off,
               nodes[static_cast<size_t>(node_xy)] + off,
               static_cast<size_t>(sc_size));
      } else if (erased.count(node_sw) != 0) {
        if (UncoupledFromCoupled(nodes, x, y, z, z_vec.data(), uncoupled,
                                 sc_size) != 0) {
          return -1;
        }
      }
    }
  }

  return DecodeUncoupled(erased, z, uncoupled, sc_size);
}

int
ClayCode::DecodeUncoupled(const std::set<int>& erased, int z,
                          std::vector<std::vector<char>>& uncoupled,
                          int sc_size)
{
  std::map<int, char*> known;
  std::map<int, char*> all;
  const size_t off = static_cast<size_t>(z) * static_cast<size_t>(sc_size);

  for (int i = 0; i < q_ * t_; ++i) {
    char* slice = uncoupled[static_cast<size_t>(i)].data() + off;
    all[i] = slice;

    if (erased.count(i) == 0) {
      known[i] = slice;
    }
  }

  if (mds_->Decode(known, all, sc_size) != 0) {
    return -1;
  }

  return 0;
}

void
ClayCode::SetPlaneOrder(std::vector<int>& order,
                        const std::set<int>& erased) const
{
  std::vector<int> z_vec(static_cast<size_t>(t_), 0);

  for (int z = 0; z < sub_chunk_no_; ++z) {
    PlaneVector(z, z_vec.data());
    order[static_cast<size_t>(z)] = 0;

    for (int i : erased) {
      if ((i % q_) == z_vec[static_cast<size_t>(i / q_)]) {
        order[static_cast<size_t>(z)] += 1;
      }
    }
  }
}

int
ClayCode::RecoverType1(std::vector<char*>& nodes, int x, int y, int z,
                       const int* z_vec, std::vector<std::vector<char>>& uncoupled,
                       int sc_size)
{
  const int node_xy = y * q_ + x;
  const int node_sw = y * q_ + z_vec[y];
  const int z_sw = z + (x - z_vec[y]) * Pow(q_, t_ - 1 - y);
  const size_t off = static_cast<size_t>(z) * static_cast<size_t>(sc_size);
  const size_t off_sw = static_cast<size_t>(z_sw) * static_cast<size_t>(sc_size);
  std::vector<char> scratch(static_cast<size_t>(sc_size), 0);
  int i0 = 0;
  int i1 = 1;
  int i2 = 2;
  int i3 = 3;

  if (z_vec[y] > x) {
    i0 = 1;
    i1 = 0;
    i2 = 3;
    i3 = 2;
  }

  std::map<int, char*> known;
  std::map<int, char*> decoded;
  known[i1] = nodes[static_cast<size_t>(node_sw)] + off_sw;
  known[i2] = uncoupled[static_cast<size_t>(node_xy)].data() + off;
  decoded[i0] = nodes[static_cast<size_t>(node_xy)] + off;
  decoded[i1] = known[i1];
  decoded[i2] = known[i2];
  decoded[i3] = scratch.data();

  if (pft_->Decode(known, decoded, sc_size) != 0) {
    return -1;
  }

  return 0;
}

int
ClayCode::CoupledFromUncoupled(std::vector<char*>& nodes, int x, int y, int z,
                               const int* z_vec,
                               std::vector<std::vector<char>>& uncoupled,
                               int sc_size)
{
  if (z_vec[y] >= x) {
    return -1;
  }

  const int node_xy = y * q_ + x;
  const int node_sw = y * q_ + z_vec[y];
  const int z_sw = z + (x - z_vec[y]) * Pow(q_, t_ - 1 - y);
  const size_t off = static_cast<size_t>(z) * static_cast<size_t>(sc_size);
  const size_t off_sw = static_cast<size_t>(z_sw) * static_cast<size_t>(sc_size);
  std::map<int, char*> known;
  std::map<int, char*> decoded;
  known[2] = uncoupled[static_cast<size_t>(node_xy)].data() + off;
  known[3] = uncoupled[static_cast<size_t>(node_sw)].data() + off_sw;
  decoded[0] = nodes[static_cast<size_t>(node_xy)] + off;
  decoded[1] = nodes[static_cast<size_t>(node_sw)] + off_sw;
  decoded[2] = known[2];
  decoded[3] = known[3];

  if (pft_->Decode(known, decoded, sc_size) != 0) {
    return -1;
  }

  return 0;
}

int
ClayCode::UncoupledFromCoupled(std::vector<char*>& nodes, int x, int y, int z,
                               const int* z_vec,
                               std::vector<std::vector<char>>& uncoupled,
                               int sc_size)
{
  const int node_xy = y * q_ + x;
  const int node_sw = y * q_ + z_vec[y];
  const int z_sw = z + (x - z_vec[y]) * Pow(q_, t_ - 1 - y);
  const size_t off = static_cast<size_t>(z) * static_cast<size_t>(sc_size);
  const size_t off_sw = static_cast<size_t>(z_sw) * static_cast<size_t>(sc_size);
  int i0 = 0;
  int i1 = 1;
  int i2 = 2;
  int i3 = 3;

  if (z_vec[y] > x) {
    i0 = 1;
    i1 = 0;
    i2 = 3;
    i3 = 2;
  }

  std::map<int, char*> known;
  std::map<int, char*> decoded;
  known[i0] = nodes[static_cast<size_t>(node_xy)] + off;
  known[i1] = nodes[static_cast<size_t>(node_sw)] + off_sw;
  decoded[0] = (i0 == 0) ? known[i0] : known[i1];
  decoded[1] = (i1 == 1) ? known[i1] : known[i0];
  decoded[i2] = uncoupled[static_cast<size_t>(node_xy)].data() + off;
  decoded[i3] = uncoupled[static_cast<size_t>(node_sw)].data() + off_sw;

  if (pft_->Decode(known, decoded, sc_size) != 0) {
    return -1;
  }

  return 0;
}

int
ClayCode::MaxIscore(const std::set<int>& erased) const
{
  std::vector<int> weight(static_cast<size_t>(t_), 0);
  int iscore = 0;

  for (int i : erased) {
    const int y = i / q_;

    if (weight[static_cast<size_t>(y)] == 0) {
      weight[static_cast<size_t>(y)] = 1;
      ++iscore;
    }
  }

  return iscore;
}

void
ClayCode::PlaneVector(int z, int* z_vec) const
{
  for (int i = 0; i < t_; ++i) {
    z_vec[t_ - 1 - i] = z % q_;
    z = (z - z_vec[t_ - 1 - i]) / q_;
  }
}

std::vector<ClayCode::Range>
ClayCode::RepairRangesNode(int lost_node) const
{
  const int y_lost = lost_node / q_;
  const int x_lost = lost_node % q_;
  const int seq_sc_count = Pow(q_, t_ - 1 - y_lost);
  const int num_seq = Pow(q_, y_lost);
  std::vector<Range> ranges;
  int index = x_lost * seq_sc_count;

  for (int seq = 0; seq < num_seq; ++seq) {
    ranges.push_back(Range{index, seq_sc_count});
    index += q_ * seq_sc_count;
  }

  return ranges;
}

std::vector<ClayCode::Range>
ClayCode::RepairRanges(int lost) const
{
  if ((lost < 0) || (lost >= k_ + m_)) {
    return {};
  }

  const int lost_node = (lost < k_) ? lost : lost + nu_;
  return RepairRangesNode(lost_node);
}

int
ClayCode::RepairSubchunkCount(int lost) const
{
  std::vector<int> weight(static_cast<size_t>(t_), 0);

  if ((lost >= 0) && (lost < k_ + m_)) {
    weight[static_cast<size_t>(lost / q_)] = 1;
  }

  int kept = 1;

  for (int y = 0; y < t_; ++y) {
    kept *= (q_ - weight[static_cast<size_t>(y)]);
  }

  return sub_chunk_no_ - kept;
}

int
ClayCode::RepairBytes(int chunk_size) const
{
  if ((q_ <= 0) || (Prepare(chunk_size) < 0) || ((chunk_size % q_) != 0)) {
    return -1;
  }

  return chunk_size / q_;
}

std::vector<int>
ClayCode::RepairHelpers(int lost) const
{
  std::vector<int> helpers;

  if ((lost < 0) || (lost >= k_ + m_)) {
    return helpers;
  }

  const int lost_node = (lost < k_) ? lost : lost + nu_;

  for (int j = 0; j < q_; ++j) {
    if (j == (lost_node % q_)) {
      continue;
    }

    const int rep = (lost_node / q_) * q_ + j;

    if (rep < k_) {
      helpers.push_back(rep);
    } else if (rep >= k_ + nu_) {
      helpers.push_back(rep - nu_);
    }
  }

  for (int chunk = 0; (chunk < k_ + m_) &&
       (static_cast<int>(helpers.size()) < d_); ++chunk) {
    if (chunk == lost) {
      continue;
    }

    bool present = false;

    for (int helper : helpers) {
      if (helper == chunk) {
        present = true;
        break;
      }
    }

    if (!present) {
      helpers.push_back(chunk);
    }
  }

  return helpers;
}

int
ClayCode::Repair(int lost, const std::vector<int>& helpers, char** helper_data,
                 char* recovered, int chunk_size)
{
  if ((recovered == nullptr) || (helper_data == nullptr) ||
      (Prepare(chunk_size) < 0)) {
    return -1;
  }

  if ((lost < 0) || (lost >= k_ + m_) ||
      (static_cast<int>(helpers.size()) != d_)) {
    return -1;
  }

  const int repair_bytes = RepairBytes(chunk_size);
  const int expect = RepairSubchunkCount(lost);

  if ((repair_bytes <= 0) || (expect <= 0) ||
      ((repair_bytes % expect) != 0)) {
    return -1;
  }

  const int lost_node = (lost < k_) ? lost : lost + nu_;
  std::map<int, char*> helper_nodes;
  std::set<int> aloof;
  std::vector<std::vector<char>> shortened;

  if (nu_ > 0) {
    shortened.assign(static_cast<size_t>(nu_),
                     std::vector<char>(static_cast<size_t>(repair_bytes), 0));
  }

  std::vector<char> used(static_cast<size_t>(k_ + m_), 0);

  for (int i = 0; i < d_; ++i) {
    const int logical = helpers[static_cast<size_t>(i)];

    if ((logical < 0) || (logical >= k_ + m_) || (logical == lost) ||
        (helper_data[i] == nullptr) || (used[static_cast<size_t>(logical)] != 0)) {
      return -1;
    }

    used[static_cast<size_t>(logical)] = 1;
    const int node = (logical < k_) ? logical : logical + nu_;
    helper_nodes[node] = helper_data[i];
  }

  for (int logical = 0; logical < k_ + m_; ++logical) {
    if ((logical == lost) || (used[static_cast<size_t>(logical)] != 0)) {
      continue;
    }

    const int node = (logical < k_) ? logical : logical + nu_;
    aloof.insert(node);
  }

  for (int i = 0; i < nu_; ++i) {
    helper_nodes[k_ + i] = shortened[static_cast<size_t>(i)].data();
  }

  memset(recovered, 0, static_cast<size_t>(chunk_size));
  std::map<int, char*> recovered_nodes;
  recovered_nodes[lost_node] = recovered;
  const std::vector<Range> ranges = RepairRangesNode(lost_node);
  std::vector<std::vector<char>> uncoupled(
    static_cast<size_t>(q_ * t_),
    std::vector<char>(static_cast<size_t>(chunk_size), 0));

  if ((static_cast<int>(helper_nodes.size()) +
       static_cast<int>(aloof.size()) +
       static_cast<int>(recovered_nodes.size())) != (q_ * t_)) {
    return -1;
  }

  return RepairOne(recovered_nodes, aloof, helper_nodes, repair_bytes, ranges,
                   uncoupled);
}

int
ClayCode::RepairOne(std::map<int, char*>& recovered, const std::set<int>& aloof,
                    std::map<int, char*>& helpers, int repair_bytes,
                    const std::vector<Range>& ranges,
                    std::vector<std::vector<char>>& uncoupled)
{
  const int repair_subchunks = sub_chunk_no_ / q_;

  if ((repair_subchunks <= 0) || ((repair_bytes % repair_subchunks) != 0)) {
    return -1;
  }

  const int sc_size = repair_bytes / repair_subchunks;
  std::vector<int> z_vec(static_cast<size_t>(t_), 0);
  std::map<int, std::set<int>> ordered_planes;
  std::map<int, int> plane_to_index;
  int plane_ind = 0;

  for (const Range& range : ranges) {
    for (int j = range.index; j < range.index + range.count; ++j) {
      PlaneVector(j, z_vec.data());
      int order = 0;

      for (const auto& item : recovered) {
        if ((item.first % q_) == z_vec[static_cast<size_t>(item.first / q_)]) {
          ++order;
        }
      }

      for (int node : aloof) {
        if ((node % q_) == z_vec[static_cast<size_t>(node / q_)]) {
          ++order;
        }
      }

      if (order <= 0) {
        return -1;
      }

      ordered_planes[order].insert(j);
      plane_to_index[j] = plane_ind;
      ++plane_ind;
    }
  }

  if (plane_ind != repair_subchunks) {
    return -1;
  }

  if (recovered.size() != 1) {
    return -1;
  }

  const int lost_chunk = recovered.begin()->first;
  std::set<int> erasures;

  for (int i = 0; i < q_; ++i) {
    erasures.insert(lost_chunk - (lost_chunk % q_) + i);
  }

  for (int node : aloof) {
    erasures.insert(node);
  }

  if (static_cast<int>(erasures.size()) > m_) {
    return -1;
  }

  std::vector<char> scratch(static_cast<size_t>(sc_size), 0);

  for (int order = 1;; ++order) {
    auto planes = ordered_planes.find(order);

    if (planes == ordered_planes.end()) {
      break;
    }

    for (int z : planes->second) {
      PlaneVector(z, z_vec.data());

      for (int y = 0; y < t_; ++y) {
        for (int x = 0; x < q_; ++x) {
          const int node_xy = y * q_ + x;

          if (erasures.count(node_xy) != 0) {
            continue;
          }

          if (helpers.count(node_xy) == 0) {
            return -1;
          }

          const int z_sw = z + (x - z_vec[static_cast<size_t>(y)]) *
                           Pow(q_, t_ - 1 - y);
          const int node_sw = y * q_ + z_vec[static_cast<size_t>(y)];
          int i0 = 0;
          int i1 = 1;
          int i2 = 2;
          int i3 = 3;

          if (z_vec[static_cast<size_t>(y)] > x) {
            i0 = 1;
            i1 = 0;
            i2 = 3;
            i3 = 2;
          }

          auto z_at = plane_to_index.find(z);

          if (z_at == plane_to_index.end()) {
            return -1;
          }

          if (aloof.count(node_sw) != 0) {
            auto zsw_at = plane_to_index.find(z_sw);

            if (zsw_at == plane_to_index.end()) {
              return -1;
            }

            std::map<int, char*> known;
            std::map<int, char*> decoded;
            known[i0] = helpers[node_xy] +
                        static_cast<size_t>(z_at->second) * static_cast<size_t>(sc_size);
            known[i3] = uncoupled[static_cast<size_t>(node_sw)].data() +
                        static_cast<size_t>(z_sw) * static_cast<size_t>(sc_size);
            decoded[i0] = known[i0];
            decoded[i1] = scratch.data();
            decoded[i2] = uncoupled[static_cast<size_t>(node_xy)].data() +
                          static_cast<size_t>(z) * static_cast<size_t>(sc_size);
            decoded[i3] = known[i3];

            if ((i0 == 0) && (decoded.find(1) == decoded.end())) {
              decoded[1] = scratch.data();
            }

            // The pairwise code always has shards 0..3. Fill any slot that
            // the index swap did not already cover.
            if (decoded.count(0) == 0) {
              decoded[0] = scratch.data();
            }

            if (decoded.count(1) == 0) {
              decoded[1] = scratch.data();
            }

            if (decoded.count(2) == 0) {
              decoded[2] = scratch.data();
            }

            if (decoded.count(3) == 0) {
              decoded[3] = scratch.data();
            }

            if (pft_->Decode(known, decoded, sc_size) != 0) {
              return -1;
            }
          } else if (z_vec[static_cast<size_t>(y)] != x) {
            if (helpers.count(node_sw) == 0) {
              return -1;
            }

            auto zsw_at = plane_to_index.find(z_sw);

            if (zsw_at == plane_to_index.end()) {
              return -1;
            }

            std::map<int, char*> known;
            std::map<int, char*> decoded;
            known[i0] = helpers[node_xy] +
                        static_cast<size_t>(z_at->second) * static_cast<size_t>(sc_size);
            known[i1] = helpers[node_sw] +
                        static_cast<size_t>(zsw_at->second) * static_cast<size_t>(sc_size);
            decoded[i0] = known[i0];
            decoded[i1] = known[i1];
            decoded[i2] = uncoupled[static_cast<size_t>(node_xy)].data() +
                          static_cast<size_t>(z) * static_cast<size_t>(sc_size);
            decoded[i3] = scratch.data();

            if (decoded.count(0) == 0) {
              decoded[0] = scratch.data();
            }

            if (decoded.count(1) == 0) {
              decoded[1] = scratch.data();
            }

            if (decoded.count(2) == 0) {
              decoded[2] = scratch.data();
            }

            if (decoded.count(3) == 0) {
              decoded[3] = scratch.data();
            }

            if (pft_->Decode(known, decoded, sc_size) != 0) {
              return -1;
            }
          } else {
            memcpy(uncoupled[static_cast<size_t>(node_xy)].data() +
                   static_cast<size_t>(z) * static_cast<size_t>(sc_size),
                   helpers[node_xy] +
                   static_cast<size_t>(z_at->second) * static_cast<size_t>(sc_size),
                   static_cast<size_t>(sc_size));
          }
        }
      }

      if (DecodeUncoupled(erasures, z, uncoupled, sc_size) != 0) {
        return -1;
      }

      for (int i : erasures) {
        const int x = i % q_;
        const int y = i / q_;
        const int node_sw = y * q_ + z_vec[static_cast<size_t>(y)];
        const int z_sw = z + (x - z_vec[static_cast<size_t>(y)]) *
                         Pow(q_, t_ - 1 - y);

        if (aloof.count(i) != 0) {
          continue;
        }

        if (x == z_vec[static_cast<size_t>(y)]) {
          memcpy(recovered[i] + static_cast<size_t>(z) * static_cast<size_t>(sc_size),
                 uncoupled[static_cast<size_t>(i)].data() +
                 static_cast<size_t>(z) * static_cast<size_t>(sc_size),
                 static_cast<size_t>(sc_size));
          continue;
        }

        if ((y != (lost_chunk / q_)) || (node_sw != lost_chunk) ||
            (helpers.count(i) == 0)) {
          return -1;
        }

        auto z_at = plane_to_index.find(z);

        if (z_at == plane_to_index.end()) {
          return -1;
        }

        int i0 = 0;
        int i1 = 1;
        int i2 = 2;
        int i3 = 3;

        if (z_vec[static_cast<size_t>(y)] > x) {
          i0 = 1;
          i1 = 0;
          i2 = 3;
          i3 = 2;
        }

        std::map<int, char*> known;
        std::map<int, char*> decoded;
        known[i0] = helpers[i] +
                    static_cast<size_t>(z_at->second) * static_cast<size_t>(sc_size);
        known[i2] = uncoupled[static_cast<size_t>(i)].data() +
                    static_cast<size_t>(z) * static_cast<size_t>(sc_size);
        decoded[i0] = known[i0];
        decoded[i1] = recovered[node_sw] +
                      static_cast<size_t>(z_sw) * static_cast<size_t>(sc_size);
        decoded[i2] = known[i2];
        decoded[i3] = scratch.data();

        if (decoded.count(0) == 0) {
          decoded[0] = scratch.data();
        }

        if (decoded.count(1) == 0) {
          decoded[1] = scratch.data();
        }

        if (decoded.count(2) == 0) {
          decoded[2] = scratch.data();
        }

        if (decoded.count(3) == 0) {
          decoded[3] = scratch.data();
        }

        if (pft_->Decode(known, decoded, sc_size) != 0) {
          return -1;
        }
      }
    }
  }

  return 0;
}

EOSFSTNAMESPACE_END
