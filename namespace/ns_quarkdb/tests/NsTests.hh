//------------------------------------------------------------------------------
// File: NsTests.hh
// Author: Cedric Caffy <cedric.caffy@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#ifndef EOS_NSTESTS_HH
#define EOS_NSTESTS_HH

#include "Namespace.hh"
#include "qclient/Members.hh"
#include "namespace/ns_quarkdb/NamespaceGroup.hh"
#include "common/RWMutex.hh"
#include <memory>

namespace qclient
{
class QClient;
}

namespace eos
{
class IContainerMDSvc;
class IFileMDSvc;
class IView;
class IFsView;
class MetadataFlusher;
class IFileMD;
}

EOSNSTESTING_BEGIN

//------------------------------------------------------------------------------
//! Class FlushAllOnConstruction
//------------------------------------------------------------------------------
class FlushAllOnConstruction
{
public:
  FlushAllOnConstruction(const QdbContactDetails& cd);
  ~FlushAllOnConstruction();

private:
  QdbContactDetails contactDetails;
};

//------------------------------------------------------------------------------
//! Test fixture providing generic utilities and initialization / destruction
//! boilerplate code
//------------------------------------------------------------------------------

typedef uint64_t (*SizeMapper)(const IFileMD* file);

class NsTests {
public:
  NsTests();
  virtual ~NsTests();

  // Lazy initialization.
  eos::IContainerMDSvc* containerSvc();
  eos::IFileMDSvc* fileSvc();
  eos::IView* view();
  eos::IFsView* fsview();

  void shut_down_everything();

  // explicit transfer of ownership
  std::unique_ptr<qclient::QClient> createQClient();

  // get a contact details object
  QdbContactDetails getContactDetails();

  // Return test cluster members
  qclient::Members getMembers();

  // Return default qclient instance. Use if you need just one qclient, and
  // you're too lazy to call createQClient.
  qclient::QClient& qcl();

  // Return the namespaces' executor
  folly::Executor* executor();

  // Return flushers
  eos::MetadataFlusher* mdFlusher();
  eos::MetadataFlusher* quotaFlusher();

  // Register size mapper
  void setSizeMapper(SizeMapper sizeMapper);

  // Populate namespace with dummy test data.
  void populateDummyData1();

  void cleanNSCache();
protected:
  eos::common::RWMutex nsMutex;
  void initServices();

  std::map<std::string, std::string> testconfig;
  std::unique_ptr<eos::ns::testing::FlushAllOnConstruction> guard;

  std::unique_ptr<eos::QuarkNamespaceGroup> namespaceGroupPtr;

  // Size mapper, if avaliable
  SizeMapper sizeMapper = nullptr;
};


EOSNSTESTING_END

#endif // EOS_NSTESTS_HH
