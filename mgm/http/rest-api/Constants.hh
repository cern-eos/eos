// ----------------------------------------------------------------------
// File: Constants.hh
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

#ifndef EOS_RESTAPI_CONSTANTS_HH
#define EOS_RESTAPI_CONSTANTS_HH

#include "mgm/Namespace.hh"

EOSMGMRESTNAMESPACE_BEGIN

//The name of the tape REST API switch on/off
static constexpr auto TAPE_REST_API_SWITCH_ON_OFF = "taperestapi.status";
//The name of the tape REST API STAGE resource switch on/off
static constexpr auto TAPE_REST_API_STAGE_SWITCH_ON_OFF = "taperestapi.stage";

// URL parameter tokens
static const inline std::string URLPARAM_ID = "{id}";

EOSMGMRESTNAMESPACE_END

#endif // EOS_RESTAPI_CONSTANTS_HH
