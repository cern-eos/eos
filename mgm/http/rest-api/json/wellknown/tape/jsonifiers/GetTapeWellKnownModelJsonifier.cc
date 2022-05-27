// ----------------------------------------------------------------------
// File: GetTapeWellKnownModelJsonifier.cc
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

#include "GetTapeWellKnownModelJsonifier.hh"

EOSMGMRESTNAMESPACE_BEGIN

void GetTapeWellKnownModelJsonifier::jsonify(const GetTapeWellKnownModel* model,
    std::stringstream& oss)
{
  Json::Value root;
  const TapeWellKnownInfos* tapeWellKnownInfos = model->getTapeWellKnownInfos();
  root["sitename"] = tapeWellKnownInfos->getSiteName();
  initializeArray(root["endpoints"]);

  for (auto& endpoint : tapeWellKnownInfos->getEndpoints()) {
    Json::Value endpointJson;
    endpointJson["uri"] = endpoint->getUri();
    endpointJson["version"] = endpoint->getVersion();
    root["endpoints"].append(endpointJson);
  }

  oss << root;
}

EOSMGMRESTNAMESPACE_END