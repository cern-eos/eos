//------------------------------------------------------------------------------
// File: MockContainerMDSvc.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
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
#pragma once
#include <gmock/gmock.h>
#include "Namespace.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"

EOSNSTESTING_BEGIN
//------------------------------------------------------------------------------
//! Class MockContainerMDSvc
//------------------------------------------------------------------------------
class MockContainerMDSvc: public ContainerMDSvc
{
public:
  MOCK_METHOD0(initialize, void());
  MOCK_METHOD1(configure, void(const std::map<std::string, std::string>& config));
  MOCK_METHOD0(finalize, void());
  MOCK_METHOD1(getContainerMD,
               std::shared_ptr<eos::IContainerMD>(eos::IContainerMD::id_t id));
  MOCK_METHOD0(createContainer, std::shared_ptr<eos::IContainerMD>());
  MOCK_METHOD1(updateStore, void(eos::IContainerMD* obj));
  MOCK_METHOD1(removeContainer, void(eos::IContainerMD* obj));
  MOCK_METHOD0(getNumContainers, uint64_t());
  MOCK_METHOD1(addChangeListener,
               void(eos::IContainerMDChangeListener* listener));
  MOCK_METHOD1(setQuotaStats, void(eos::IQuotaStats* quota_stats));
  MOCK_METHOD2(notifyListeners, void(eos::IContainerMD*,
                                     eos::IContainerMDChangeListener::Action));
  MOCK_METHOD1(getLostFoundContainer,
               std::shared_ptr<eos::IContainerMD>(const std::string&));
  MOCK_METHOD2(createInParent,
               std::shared_ptr<eos::IContainerMD>(const std::string&,
                   eos::IContainerMD*));
  MOCK_METHOD1(setFileMDService, void(eos::IFileMDSvc* cont_svc));
  MOCK_METHOD1(setContainerAccounting, void(eos::IFileMDChangeListener*));
  MOCK_METHOD0(getFirstFreeId, eos::IContainerMD::id_t());
};

EOSNSTESTING_END
