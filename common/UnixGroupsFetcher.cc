/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                           *
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

#include "common/UnixGroupsFetcher.hh"
#include "common/VirtualIdentity.hh"
#include "common/Logging.hh"

EOSCOMMONNAMESPACE_BEGIN

std::vector<gid_t> UnixGrentFetcher::getGroups(const std::string &username, gid_t gid)
{

    std::vector<gid_t> groups;

    std::unique_lock<std::mutex> lock(mtx);
    setgrent();

    group *gr {nullptr};
    while ((gr = getgrent())) {
        int cnt = 0;
        if (gr->gr_gid == gid) {
            groups.emplace_back(gr->gr_gid);
        }

        while (gr->gr_mem[cnt] != nullptr) {
            if (!strcmp(gr->gr_mem[cnt], username.c_str())) {
                groups.emplace_back(gr->gr_gid);
            }
        }
    }

    return groups;
}

std::vector<gid_t> UnixGroupListFetcher::getGroups(const std::string &username, gid_t gid)
{
    std::vector<gid_t> groups(kDefaultMaxGroupSize);
    int ngroups = kDefaultMaxGroupSize;
    if (getgrouplist(username.c_str(), gid, groups.data(), &ngroups) == -1) {
        groups.resize(ngroups);

        if (getgrouplist(username.c_str(), gid, groups.data(), &ngroups) == -1) {
            // This is very unlikely, we can do this in a tight loop to avoid this, but very much an overkill
            eos_static_err("msg=\"Groups resized while fetching groupinfo\" uid=%s ngroups=%d",
                           username.c_str(), ngroups);
            return groups; // do not resize again as we have lesser groups!
        }
    }
    groups.resize(ngroups);
    return groups;
}

void populateGroups(const std::string& username, gid_t gid,
                    VirtualIdentity &vid,
                    UnixGroupsFetcher * const fetcher)
{
    if (fetcher == nullptr) {
        eos_static_crit("msg=\"Cannot populate groups information! Uninitialized Fetcher\"");
        return;
    }

    auto group_list = fetcher->getGroups(username,gid);
    if (group_list.empty()) {
        eos_static_err("msg=\"No groups found for user\" name=\"%s\" gid=%d", username.c_str(), gid);
        return;
    }

    vid.allowed_gids.insert(group_list.begin(), group_list.end());
}


EOSCOMMONNAMESPACE_END
