// ----------------------------------------------------------------------
// File: ControllerManager.hh
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
#ifndef EOS_CONTROLLERMANAGER_HH
#define EOS_CONTROLLERMANAGER_HH

#include "mgm/Namespace.hh"
#include <map>
#include <memory>
#include "mgm/http/rest-api/controllers/Controller.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class keeps track of the controllers of a REST API
 */
class ControllerManager {
public:
  ControllerManager();
  /**
   * Adds a controller to this manager
   * @param controller the controller to add
   */
  void addController(std::shared_ptr<Controller> controller);
  /**
   * Returns the controller depending on the client URL
   * e.g: if the client's URL is /api/v1/stage/xxx/cancel, the controller that will be returned will be
   * the STAGEv1 controller
   * @param clientUrl the URL from the client allowing to determine the controller to get
   * @return the controller that depends on the client URL
   */
  std::shared_ptr<Controller> getController(const std::string & clientUrl) const;
private:
  //Map associating a URL and a controller
  std::map<std::string,std::shared_ptr<Controller>> mControllers;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CONTROLLERMANAGER_HH
