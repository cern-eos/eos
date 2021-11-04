// ----------------------------------------------------------------------
// File: ErrorModel.hh
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

#ifndef EOS_MODEL_HH
#define EOS_MODEL_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/json/tape/TapeModelJsonifier.hh"
#include <memory>
#include "mgm/http/rest-api/json/tape/JsonCPPTapeModelJsonifier.hh"
#include <sstream>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * Base class for a REST-API model object
 *
 * A Model object represents client's request or api response.
 */
class Model {
public:
  Model(){
    mJsonifier.reset(new JsonCPPTapeModelJsonifier());
  }
  virtual void jsonify(std::stringstream & ss) const = 0;
  virtual ~Model(){}
protected:
  /**
   * Jsonifier object that allows to jsonify this object
   */
  std::unique_ptr<TapeModelJsonifier> mJsonifier;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_MODEL_HH
