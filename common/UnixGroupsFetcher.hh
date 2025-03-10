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
 ***********************************************************************/

#ifndef EOS_UNIXGROUPSFETCHER_HH
#define EOS_UNIXGROUPSFETCHER_HH

#include "common/Namespace.hh"
#include <grp.h>
#include <string>
#include <vector>
#include <mutex>

EOSCOMMONNAMESPACE_BEGIN

class VirtualIdentity;
// constants
static constexpr int kDefaultMaxGroupSize = 16;

struct UnixGroupsFetcher
{
    virtual ~UnixGroupsFetcher() = default;
    virtual std::vector<gid_t> getGroups(const std::string& username, gid_t gid) = 0;
};

class UnixGrentFetcher: public UnixGroupsFetcher {
public:
    std::vector<gid_t> getGroups(const std::string& username, gid_t gid) override;
private:
    std::mutex mtx;  //< mutex to protect the access to the getgrent() function
};

struct UnixGroupListFetcher: public UnixGroupsFetcher {
    std::vector<gid_t> getGroups(const std::string& username, gid_t gid) override;
};


void populateGroups(const std::string& username, gid_t gid,
                    VirtualIdentity& vid, UnixGroupsFetcher * const  fetcher);

EOSCOMMONNAMESPACE_END

#endif //UNIXGROUPSFETCHER_HH
