// ----------------------------------------------------------------------
// File: GetStageBulkRequestResponseModel.hh
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

#ifndef EOS_GETSTAGEBULKREQUESTRESPONSEMODEL_HH
#define EOS_GETSTAGEBULKREQUESTRESPONSEMODEL_HH

#include "mgm/Namespace.hh"
#include "common/json/Jsonifiable.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/bulk-request/BulkRequest.hh"

EOSMGMRESTNAMESPACE_BEGIN

class GetStageBulkRequestResponseModel : public common::Jsonifiable<GetStageBulkRequestResponseModel> {
public:
  class Item {
  public:
    std::string mPath;
    std::string mError;
    bool mOnDisk;
  };
  GetStageBulkRequestResponseModel(){}
  void addItem(std::unique_ptr<Item> && item);
  const std::vector<std::unique_ptr<Item>> & getItems() const;
private:
  std::vector<std::unique_ptr<Item>> mItems;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_GETSTAGEBULKREQUESTRESPONSEMODEL_HH
