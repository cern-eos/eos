// ----------------------------------------------------------------------
// File: TapeRestApiWellKnownController.cc
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

#include "TapeWellKnownController.hh"
#include "mgm/http/rest-api/exception/NotFoundException.hh"
#include "mgm/http/rest-api/exception/MethodNotAllowedException.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeWellKnownController::TapeWellKnownController(const std::string& accessURL):
  Controller(accessURL) {}

common::HttpResponse*
TapeWellKnownController::handleRequest(common::HttpRequest* request,
                                       const common::VirtualIdentity* vid)
{
  try {
    return mControllerActionDispatcher.getAction(request)->run(request, vid);
  } catch (const NotFoundException& ex) {
    eos_static_info(ex.what());
    return mTapeRestApiResponseFactory.createNotFoundError().getHttpResponse();
  } catch (const MethodNotAllowedException& ex) {
    eos_static_info(ex.what());
    return mTapeRestApiResponseFactory.createMethodNotAllowedError(
             ex.what()).getHttpResponse();
  } catch (...) {
    std::string errorMsg = "Unknown exception occured";
    eos_static_err(errorMsg.c_str());
    return mTapeRestApiResponseFactory.createInternalServerError(
             errorMsg).getHttpResponse();
  }
}

EOSMGMRESTNAMESPACE_END