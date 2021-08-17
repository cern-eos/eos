//------------------------------------------------------------------------------
//! @file QueryPrepareResult.hh
//! @author Cedric Caffy - CERN
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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/
#ifndef EOS_QUERYPREPARERESULT_HH
#define EOS_QUERYPREPARERESULT_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * Class holding the result of the query prepare execution
 * It contains the response that will be returned to the client and the return code that
 * is set during the execution of the query prepare method of the PrepareManager.
 */
class QueryPrepareResult {
public:
  /**
   * The PrepareManager need to acess the setter methods
   */
  friend class PrepareManager;
  QueryPrepareResult();
  const bool hasQueryPrepareFinished() const;
  std::shared_ptr<QueryPrepareResponse> getResponse() const;
  const int getReturnCode() const;
private:
  void setQueryPrepareFinished();
  void setReturnCode(int returnCode);
  bool mHasQueryPrepareFinished;
  std::shared_ptr<QueryPrepareResponse> mResponse;
  int mReturnCode;
};

EOSBULKNAMESPACE_END

#endif // EOS_QUERYPREPARERESULT_HH
