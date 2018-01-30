//------------------------------------------------------------------------------
// File: TestUtils.hh
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
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

#include "Namespace.hh"
EOSNSTESTING_BEGIN

//------------------------------------------------------------------------------
//! Class FlushAllOnDestruction
//------------------------------------------------------------------------------
class FlushAllOnDestruction {
public:
  FlushAllOnDestruction(const qclient::Members &mbr) : members(mbr) {
    qclient::RetryStrategy strategy {true, std::chrono::seconds(10)};
    qclient::QClient qcl(members, true, strategy);
    qcl.exec("FLUSHALL").get();
  }

  ~FlushAllOnDestruction() {
    qclient::RetryStrategy strategy {true, std::chrono::seconds(10)};
    qclient::QClient qcl(members, true, strategy);
    qcl.exec("FLUSHALL").get();
  }

private:
  qclient::Members members;
};

EOSNSTESTING_END
