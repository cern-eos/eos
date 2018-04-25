/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Metadata prefetching engine
//------------------------------------------------------------------------------

#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/Prefetcher.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Prefetcher::Prefetcher(IFileMDSvc *file_svc, IContainerMDSvc *cont_svc) {
  pFileMDSvc = file_svc;
  pContainerMDSvc = cont_svc;
}

//------------------------------------------------------------------------------
// Declare an intent to access FileMD with the given id soon
//------------------------------------------------------------------------------
void Prefetcher::stageFileMD(id_t id) {
  mFileMDs.emplace_back(pFileMDSvc->getFileMDFut(id));
}

//------------------------------------------------------------------------------
// Declare an intent to access ContainerMD with the given id soon
//------------------------------------------------------------------------------
void Prefetcher::stageContainerMD(id_t id) {
  mContainerMDs.emplace_back(pContainerMDSvc->getContainerMDFut(id));
}

//------------------------------------------------------------------------------
// Wait until all staged requests have been loaded in cache.
//------------------------------------------------------------------------------
void Prefetcher::wait() {
  for(size_t i = 0; i < mFileMDs.size(); i++) {
    mFileMDs[i].wait();
  }

  for(size_t i = 0; i < mContainerMDs.size(); i++) {
    mContainerMDs[i].wait();
  }
}


EOSNSNAMESPACE_END
