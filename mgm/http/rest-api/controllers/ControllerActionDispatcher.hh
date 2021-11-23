// ----------------------------------------------------------------------
// File: ControllerActionDispatcher.hh
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
#ifndef EOS_CONTROLLERACTIONDISPATCHER_HH
#define EOS_CONTROLLERACTIONDISPATCHER_HH

#include "mgm/Namespace.hh"
#include <string>
#include <map>
#include "mgm/http/HttpHandler.hh"
#include <functional>
#include "common/http/HttpResponse.hh"
#include "common/VirtualIdentity.hh"


EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class holds a map of <<URL,HttpMethod>,std::function> allowing to identify
 * which function to run on a specific URL,method
 */
class ControllerActionDispatcher {
public:
  typedef std::function<common::HttpResponse *(common::HttpRequest * request,const common::VirtualIdentity * vid)> ControllerHandler;
  typedef std::map<std::string,std::map<common::HttpHandler::Methods,ControllerHandler>> URLMethodFunctionMap;

  ControllerActionDispatcher() = default;
  /**
   * Set a ControllerHandler function to a specific URL pattern and Http method
   * @param urlPattern the URL (with possible parameters) associated to the handler to run
   * @param method Http method associated with the URL and the handler to run
   * @param controllerHandler the handler function that corresponds to the URL and the method
   */
  void addAction(const std::string & urlPattern, const common::HttpHandler::Methods method, const ControllerHandler & controllerHandler);
  /**
   * Returns the handler depending on the URL and the Http method located in the request passed in parameter
   * @param request the request allowing to return the handler
   * @return the handler depending on the URL and the Http method located in the request passed in parameter
   */
  ControllerHandler getAction(common::HttpRequest * request);
private:
  //The map storing the URL,HttpMethod and the associated handler
  URLMethodFunctionMap mURLMapMethodFunctionMap;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CONTROLLERACTIONDISPATCHER_HH
