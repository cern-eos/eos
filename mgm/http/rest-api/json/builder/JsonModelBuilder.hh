// ----------------------------------------------------------------------
// File: JsonModelBuilder.hh
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
#ifndef EOS_JSONMODELBUILDER_HH
#define EOS_JSONMODELBUILDER_HH

#include "mgm/Namespace.hh"
#include <memory>
#include <string>
#include <vector>
#include <utility>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class allows to build a Model object
 * from a json string.
 * @tparam Model the object to create from the JSON string
 */
template<typename Model>
class JsonModelBuilder
{
public:
  /**
   * Returns a unique_ptr to the Model object created from the JSON string passed
   * in parameter
   * @param json the JSON string from which the Model object should be created from
   * @return the unique_ptr to the Model corresponding to the JSON string passed
   * in parameter
   */
  virtual std::unique_ptr<Model> buildFromJson(const std::string& json) = 0;
  virtual ~JsonModelBuilder() {}
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_JSONMODELBUILDER_HH
