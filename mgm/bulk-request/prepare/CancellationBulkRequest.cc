//------------------------------------------------------------------------------
//! @file CancellationBulkRequest.cc
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
#include <common/Logging.hh>
#include "CancellationBulkRequest.hh"

EOSBULKNAMESPACE_BEGIN

CancellationBulkRequest::CancellationBulkRequest(const std::string& id): BulkRequest(id){}

const BulkRequest::Type CancellationBulkRequest::getType() const {
  return BulkRequest::Type::PREPARE_CANCEL;
}

void CancellationBulkRequest::addFile(std::unique_ptr<File>&& file) {
  if(!file->getError()) {
    file->setState(File::State::CANCELLED);
  }
  BulkRequest::addFile(std::move(file));
}

EOSBULKNAMESPACE_END