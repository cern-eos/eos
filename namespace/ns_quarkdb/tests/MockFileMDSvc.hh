//------------------------------------------------------------------------------
// File: MockFileMDSvc.cc
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
#include "namespace/interface/IFileMDSvc.hh"

EOSNSTESTING_BEGIN
//------------------------------------------------------------------------------
//! Class MockFileMDSvc
//------------------------------------------------------------------------------
class MockFileMDSvc: public IFileMDSvc
{
public:
  MOCK_METHOD0(initialize, void());
  MOCK_METHOD1(configure, void(const std::map<std::string, std::string>& config));
  MOCK_METHOD0(finalize, void());
  MOCK_METHOD1(getFileMD, std::shared_ptr<eos::IFileMD>(eos::IFileMD::id_t id));
  MOCK_METHOD0(createFile, std::shared_ptr<eos::IFileMD>());
  MOCK_METHOD1(updateStore, void(eos::IFileMD* obj));
  MOCK_METHOD1(removeFile, void(eos::IFileMD* obj));
  MOCK_METHOD0(getNumFiles, uint64_t());
  MOCK_METHOD1(addChangeListener, void(eos::IFileMDChangeListener* listener));
  MOCK_METHOD1(notifyListeners, void(eos::IFileMDChangeListener::Event* event));
  MOCK_METHOD1(setQuotaStats, void(eos::IQuotaStats* quota_stats));
  MOCK_METHOD1(setContMDService, void(eos::IContainerMDSvc* cont_svc));
  MOCK_METHOD1(visit, void(eos::IFileVisitor* visitor));
  MOCK_METHOD0(getFirstFreeId, eos::IFileMD::id_t());
};

EOSNSTESTING_END
