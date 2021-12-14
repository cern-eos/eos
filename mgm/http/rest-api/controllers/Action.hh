// ----------------------------------------------------------------------
// File: Action.hh
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
#ifndef EOS_ACTION_HH
#define EOS_ACTION_HH

#include "mgm/Namespace.hh"
#include "common/VirtualIdentity.hh"
#include "common/http/HttpRequest.hh"
#include "common/http/HttpResponse.hh"
#include "common/http/HttpHandler.hh"

EOSMGMRESTNAMESPACE_BEGIN

class Action {
public:
  Action(const std::string & urlPattern,const common::HttpHandler::Methods method): mURLPattern(urlPattern),mMethod(method){};
  virtual common::HttpResponse * run(common::HttpRequest * request, const common::VirtualIdentity * vid) = 0;
  inline const std::string & getURLPattern() const { return mURLPattern; }
  inline const common::HttpHandler::Methods getMethod() const { return mMethod; }
protected:
  std::string mURLPattern;
  eos::common::HttpHandler::Methods mMethod;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_ACTION_HH
