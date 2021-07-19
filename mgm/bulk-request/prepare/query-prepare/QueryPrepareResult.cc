//------------------------------------------------------------------------------
//! @file QueryPrepareResult.cc
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
#include "QueryPrepareResult.hh"

EOSBULKNAMESPACE_BEGIN

QueryPrepareResult::QueryPrepareResult(): mHasQueryPrepareFinished(false){
  mResponse.reset(new QueryPrepareResponse());
}

const bool QueryPrepareResult::hasQueryPrepareFinished() const {
  return mHasQueryPrepareFinished;
}

std::shared_ptr<QueryPrepareResponse> QueryPrepareResult::getResponse() const {
  return mResponse;
}

void QueryPrepareResult::setQueryPrepareFinished() {
  mHasQueryPrepareFinished = true;
}

const int QueryPrepareResult::getReturnCode() const {
  return mReturnCode;
}

void QueryPrepareResult::setReturnCode(int returnCode) {
  mReturnCode = returnCode;
}

EOSBULKNAMESPACE_END