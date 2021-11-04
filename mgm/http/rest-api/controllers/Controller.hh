// ----------------------------------------------------------------------
// File: Controller.hh
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

#ifndef EOS_CONTROLLER_HH
#define EOS_CONTROLLER_HH

#include "common/VirtualIdentity.hh"
#include "common/http/HttpHandler.hh"
#include "common/http/HttpResponse.hh"
#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/controllers/ControllerActionDispatcher.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class is the base class for all the controllers of a REST API
 * A controller contains the logic that is ran when a client queries the
 * controller access URL
 */
class Controller {
public:
  /**
   * Constructor of a controller
   * @param accessURL
   */
  Controller(const std::string & accessURL);
  /**
   * This method handles the request passed in parameter. It calls the controller
   * method according to what the URL of the request is.
   * @param request the client's request
   * @param vid the virtual identity of the client
   * @return the response the client expects.
   */
  virtual common::HttpResponse * handleRequest(common::HttpRequest * request,const common::VirtualIdentity * vid) = 0;
  /**
   * Returns the access URL of this controller
   * @return the access URL of this controller
   */
  const std::string getAccessURL() const;
protected:
  /**
   * Depending on the URL coming from the client's request, the dispatcher will
   * run a method of this controller.
   * This dispatcher needs to be initialized in the constructor of the controller
   */
  ControllerActionDispatcher mControllerActionDispatcher;
  //The URL to access the functionalities of this controller
  std::string mAccessURL;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CONTROLLER_HH
