// ----------------------------------------------------------------------
// File: GetArchiveInfoResponseModel.hh
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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
#ifndef EOS_GETARCHIVEINFORESPONSEMODEL_HH
#define EOS_GETARCHIVEINFORESPONSEMODEL_HH

#include "mgm/Namespace.hh"
#include "common/json/Jsonifiable.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include <memory>
#include <optional>
#include <string>
#include <vector>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * Response model for POST /api/v1/archiveinfo (WLCG Tape REST API v1).
 */
class GetArchiveInfoResponseModel
  : public common::Jsonifiable<GetArchiveInfoResponseModel>
{
public:
  struct Entry
  {
    std::string path;
    std::optional<std::string> locality;
    std::optional<std::string> error;
  };

  GetArchiveInfoResponseModel() = default;

  explicit GetArchiveInfoResponseModel(
    const std::shared_ptr<bulk::QueryPrepareResponse>& queryPrepareResponse)
  {
    if (!queryPrepareResponse) {
      return;
    }

    for (const auto& response : queryPrepareResponse->responses) {
      Entry entry;
      entry.path = response.path;

      if (!response.is_exists) {
        if (!response.error_text.empty()) {
          entry.error = response.error_text;
        } else {
          entry.error =
            "USER ERROR: file does not exist or is not accessible to you";
        }
      } else if (!response.error_text.empty()) {
        entry.error = response.error_text;
      } else {
        entry.locality = mapLocality(response);
      }

      mEntries.push_back(std::move(entry));
    }
  }

  inline const std::vector<Entry>& getEntries() const { return mEntries; }

private:
  static std::string mapLocality(const bulk::QueryPrepareFileResponse& response)
  {
    if (response.is_online && response.is_on_tape) {
      return "DISK_AND_TAPE";
    }
    if (response.is_online) {
      return "DISK";
    }
    if (response.is_on_tape) {
      return "TAPE";
    }
    return "UNAVAILABLE";
  }

  std::vector<Entry> mEntries;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_GETARCHIVEINFORESPONSEMODEL_HH
