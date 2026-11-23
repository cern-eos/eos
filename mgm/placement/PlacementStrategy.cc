// ----------------------------------------------------------------------
// File: PlacementStrategy
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                           *
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

#include "PlacementStrategy.hh"
#include "mgm/placement/PlacementStrategy.hh"

namespace eos::mgm::placement {
size_t
PlacementStrategy::calculateMaxGeoOverlap(item_id_t candidate_id,
                                          const ClusterData &data,
                                          const PlacementResult &current_result,
                                          int items_added) const {
    if (candidate_id <= 0 || data.disk_tags.empty()) {
        return 0; // No penalty if no topology data exists
    }

    if ((size_t)candidate_id > data.disk_tags.size()) {
        return std::numeric_limits<size_t>::max();
    }

    const auto& candidate_path = data.disk_tags[candidate_id - 1];
    size_t max_overlap_found = 0;

    // Compare against ALL currently selected replicas
    for (int i = 0; i < items_added; ++i) {
        item_id_t existing_id = current_result.ids[i];

        // Skip invalid existing IDs (shouldn't happen, but doublecheck)
        if (existing_id <= 0 || (size_t)existing_id > data.disk_tags.size()) {
            continue;
        }

        const auto& existing_path = data.disk_tags[existing_id - 1];

        // 3. Calculate overlap depth for this pair
        // (e.g., DC::Room matches = 2)
        size_t current_overlap = 0;
        size_t len = std::min(candidate_path.size(), existing_path.size());

        for (size_t d = 0; d < len; ++d) {
            if (candidate_path[d] != existing_path[d]) break;
            current_overlap++;
        }

        // 4. Track the WORST overlap (closest proximity)
        if (current_overlap > max_overlap_found) {
            max_overlap_found = current_overlap;
        }
    }

    return max_overlap_found;
}

PlacementResult
PlacementStrategy::placeWithGeoFilter(const ClusterData &cluster_data,
                                      const Args &args,
                                      const std::vector<item_id_t> &sorted_candidates)
{

    PlacementResult result;
    int replicas_selected = 0;

    for (size_t i = 0; i < sorted_candidates.size(); ++i) {
        if (replicas_selected >= args.n_replicas) break;

        item_id_t candidate_id = sorted_candidates[i];

        // Caller might have validated, but we double-check for safety)
        // Do this only with disks for now! A future version will handle
        // buckets here
        if (candidate_id <= 0) continue;
        if (result.contains(candidate_id)) continue;

        // Calculate overlap with ALL currently selected replicas
        size_t overlap = calculateMaxGeoOverlap(candidate_id, cluster_data, result, replicas_selected);

        // We want to skip this candidate if it overlaps, BUT not if it causes failure.
        bool skip_candidate = false;

        if (replicas_selected > 0 && overlap > 0) {
            // Heuristic: Do we have enough candidates left to afford skipping this one?
            // We look ahead to see if there are other options.
            size_t remaining_candidates = sorted_candidates.size() - i;
            size_t needed = args.n_replicas - replicas_selected;

            // "Buffer Factor" of 2: We only skip if we have 2x more candidates than we need.
            // This ensures we don't aggressively filter ourselves into ENOSPC.
            if (remaining_candidates > needed * 2) {
                skip_candidate = true;
            }
        }

        if (!skip_candidate) {
            result.ids[replicas_selected++] = candidate_id;
        }
    }

    // 5. Finalize
    if (replicas_selected < args.n_replicas) {
        result.ret_code = ENOSPC;
        result.err_msg = "Could not find enough suitable replicas";
    } else {
        result.ret_code = 0;
    }

    return result;
}



}// namespace eos::mgm::placement
