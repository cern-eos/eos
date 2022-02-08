//------------------------------------------------------------------------------
//! @file Jsonifier.hh
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

#ifndef EOS_JSONIFIER_HH
#define EOS_JSONIFIER_HH

#include "mgm/Namespace.hh"
#include <memory>
#include <string>
#include <sstream>

EOSBULKNAMESPACE_BEGIN

class QueryPrepareResponse;

class Jsonifier {
public:
  virtual void jsonify(const QueryPrepareResponse & response, std::stringstream & oss) = 0;
};

EOSBULKNAMESPACE_END

#endif // EOS_JSONIFIER_HH
