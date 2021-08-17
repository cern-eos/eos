//------------------------------------------------------------------------------
//! @file JSONCppJsonifier.hh
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

#ifndef EOS_JSONCPPJSONIFIER_HH
#define EOS_JSONCPPJSONIFIER_HH

#include "mgm/Namespace.hh"
#include "Jsonifier.hh"
#include <json/json.h>
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * Visitor to jsonify an object using the JSONCpp library
 */
class JSONCppJsonifier : public Jsonifier {
public:
  void jsonify(const QueryPrepareResponse & response, std::stringstream & oss) override;
private:
  /**
   * Jsonify the base QueryPrepareResponse without any file
   * @param response the QueryPrepareResponse to jsonify
   * @param json the json object to add the QueryPrepareResponse object to (Json root object for example)
   */
  void jsonifyQueryPrepareResponse(const QueryPrepareResponse & response, Json::Value & json);
  /**
   * Jsonifiy one file of the QueryPrepareResponse to jsonify
   * @param fileResponse the file to jsonify
   * @param json the object to which the file will be added (responses attribute)
   */
  void jsonifyQueryPrepareResponseFile(const QueryPrepareFileResponse & fileResponse,Json::Value & json);
};

EOSBULKNAMESPACE_END
#endif // EOS_JSONCPPJSONIFIER_HH
