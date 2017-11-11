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

#pragma once
#include "namespace/ns_quarkdb/BackendClient.hh"
#include <iostream>

//------------------------------------------------------------------------------
//! Print command useage info
//!
//! @return error code EINVAL
//------------------------------------------------------------------------------
int usage_help();

//------------------------------------------------------------------------------
//! Dump metadata object information stored in QDB
//!
//! @param qcl qclient used to connect to quarkdb backend
//! @param id metadata object id
//! @param is_file if true search for file md object, otherwise for container
//!
//! @return json string representation of the metadata object
//------------------------------------------------------------------------------
std::string DumpProto(qclient::QClient* qcl, uint64_t id, bool is_file);

//------------------------------------------------------------------------------
//! Pretty print metadata object
//!
//! @param senv string representing info & separator
//------------------------------------------------------------------------------
void PrettyPrint(const std::string& senv);
