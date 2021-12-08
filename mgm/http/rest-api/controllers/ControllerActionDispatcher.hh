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
#include "mgm/http/rest-api/controllers/Action.hh"
#include <memory>


EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class holds a map of <<URL,HttpMethod>,std::function> allowing to identify
 * which function to run on a specific URL,method
 */
class ControllerActionDispatcher {
public:
  typedef std::unique_ptr<Action> ControllerHandler;
  typedef std::map<std::string,std::map<common::HttpHandler::Methods,ControllerHandler>> URLMethodFunctionMap;

  ControllerActionDispatcher() = default;
  /**
   * Set a ControllerHandler function to a specific URL pattern and Http method
   * @param urlPattern the URL (with possible parameters) associated to the handler to run
   * @param method Http method associated with the URL and the handler to run
   * @param controllerHandler the handler function that corresponds to the URL and the method
   */
  void addAction(std::unique_ptr<Action> && action);
  /**
   * Returns the handler depending on the URL and the Http method located in the urlMethodFunctionItem passed in parameter
   * @param urlMethodFunctionItem the urlMethodFunctionItem allowing to return the handler
   * @return the handler depending on the URL and the Http method located in the urlMethodFunctionItem passed in parameter
   */
  Action * getAction(common::HttpRequest * urlMethodFunctionItem);
private:
  //The map storing the URL,HttpMethod and the associated handler
  URLMethodFunctionMap mURLMapMethodFunctionMap;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CONTROLLERACTIONDISPATCHER_HH
