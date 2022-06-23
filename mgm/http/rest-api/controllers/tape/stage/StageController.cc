// ----------------------------------------------------------------------
// File: StageController.cc
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

#include "StageController.hh"

EOSMGMRESTNAMESPACE_BEGIN

StageController::StageController(const std::string & accessURL, const TapeRestApiConfig * tapeRestApiConfig):Controller(accessURL),mTapeRestApiConfig(tapeRestApiConfig){}

common::HttpResponse * StageController::handleRequest(common::HttpRequest * request,const common::VirtualIdentity * vid) {
  if(mTapeRestApiConfig->isStageEnabled()) {
    return mControllerActionDispatcher.getAction(request)->run(request, vid);
  }
  return mResponseFactory.createNotImplementedError().getHttpResponse();
}

EOSMGMRESTNAMESPACE_END