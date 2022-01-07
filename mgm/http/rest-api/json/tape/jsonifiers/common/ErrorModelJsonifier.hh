// ----------------------------------------------------------------------
// File: ErrorModelJsonifier.hh
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

#ifndef EOS_ERRORMODELJSONIFIER_HH
#define EOS_ERRORMODELJSONIFIER_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/model/tape/common/ErrorModel.hh"
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonifier.hh"
#include "common/json/JsonCppJsonifier.hh"

EOSMGMRESTNAMESPACE_BEGIN

class ErrorModelJsonifier : public TapeRestApiJsonifier<ErrorModel>, public common::JsonCppJsonifier<ErrorModel>  {
public:
  void jsonify(const ErrorModel * model, std::stringstream & ss) override;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_ERRORMODELJSONIFIER_HH
