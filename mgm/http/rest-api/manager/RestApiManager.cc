// ----------------------------------------------------------------------
// File: RestApiManager.cc
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

#include "RestApiManager.hh"

EOSMGMRESTNAMESPACE_BEGIN

RestApiManager::RestApiManager()
{
  mTapeRestApiConfig = std::make_unique<TapeRestApiConfig>();
  mMapAccessURLRestHandlerFactory[mTapeRestApiConfig->getAccessURL()] =
    std::make_unique<TapeRestHandlerFactory>(mTapeRestApiConfig.get());
}

bool RestApiManager::isRestRequest(const std::string& requestURL)
{
  const auto& restHandler = getRestHandler(requestURL);
  return (restHandler != nullptr && restHandler->isRestRequest(requestURL));
}

TapeRestApiConfig* RestApiManager::getTapeRestApiConfig()
{
  return mTapeRestApiConfig.get();
}

std::unique_ptr<rest::RestHandler> RestApiManager::getRestHandler(
  const std::string& requestURL)
{
  const auto& restHandlerFactory = std::find_if(
                                     mMapAccessURLRestHandlerFactory.begin(),
  mMapAccessURLRestHandlerFactory.end(), [&requestURL](const auto & kv) {
    return ::strncmp(kv.first.c_str(), requestURL.c_str(), kv.first.length()) == 0;
  });

  if (restHandlerFactory != mMapAccessURLRestHandlerFactory.end()) {
    return restHandlerFactory->second->createRestHandler();
  }

  return nullptr;
}

EOSMGMRESTNAMESPACE_END