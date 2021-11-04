// ----------------------------------------------------------------------
// File: TapeModelJsonifier.hh
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

#ifndef EOS_TAPEMODELJSONIFIER_HH
#define EOS_TAPEMODELJSONIFIER_HH

#include "mgm/Namespace.hh"
#include <sstream>
#include <json/json.h>

EOSMGMRESTNAMESPACE_BEGIN

class ErrorModel;
class CreatedStageBulkRequestResponseModel;

/**
 * This class allows to create the json representation
 * of objects
 */
class TapeModelJsonifier {
public:
  /**
   * Creates the json representation of an ErrorModel object
   * @param errorModel the object to create the JSON representation from
   * @param oss the stream where the json will be put on
   */
  virtual void jsonify(const ErrorModel & errorModel, std::stringstream & oss) = 0;
  virtual void jsonify(const CreatedStageBulkRequestResponseModel& createdStageBulkRequestModel, std::stringstream & oss) = 0;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPEMODELJSONIFIER_HH
