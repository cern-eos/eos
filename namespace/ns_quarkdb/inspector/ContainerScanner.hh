/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
//! @brief Class for scanning through all container metadata
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "proto/ContainerMd.pb.h"
#include <qclient/structures/QLocalityHash.hh>
#include <folly/futures/Future.h>

namespace qclient {
  class QClient;
}

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! ContainerScannerPrimitive class: No support for full paths
//------------------------------------------------------------------------------
class ContainerScannerPrimitive {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ContainerScannerPrimitive(qclient::QClient &qcl);

  //----------------------------------------------------------------------------
  //! Is the iterator valid?
  //----------------------------------------------------------------------------
  bool valid() const;

  //----------------------------------------------------------------------------
  //! Advance iterator - only call when valid() == true
  //----------------------------------------------------------------------------
  void next();

  //----------------------------------------------------------------------------
  //! Is there an error?
  //----------------------------------------------------------------------------
  bool hasError(std::string &err) const;

  //----------------------------------------------------------------------------
  //! Get current element
  //----------------------------------------------------------------------------
  bool getItem(eos::ns::ContainerMdProto &item, std::string *path = nullptr);

  //----------------------------------------------------------------------------
  //! Get number of elements scanned so far
  //----------------------------------------------------------------------------
  uint64_t getScannedSoFar() const;

private:
  qclient::QLocalityHash::Iterator mIterator;
  std::string mError;
  uint64_t mScanned = 0;
};

//------------------------------------------------------------------------------
//! ContainerScanner class: Optional support for full paths
//------------------------------------------------------------------------------
class ContainerScanner {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ContainerScanner(qclient::QClient &qcl, bool fullPaths = false, bool counts = false);

  //----------------------------------------------------------------------------
  //! Is the iterator valid?
  //----------------------------------------------------------------------------
  bool valid() const;

  //----------------------------------------------------------------------------
  //! Advance iterator - only call when valid() == true
  //----------------------------------------------------------------------------
  void next();

  //----------------------------------------------------------------------------
  //! Is there an error?
  //----------------------------------------------------------------------------
  bool hasError(std::string &err) const;

  //----------------------------------------------------------------------------
  //! Return type of getItem
  //----------------------------------------------------------------------------
  struct Item {
    eos::ns::ContainerMdProto proto;
    folly::Future<std::string> fullPath;
    folly::Future<uint64_t> fileCount;
    folly::Future<uint64_t> containerCount;

    Item() : proto(), fullPath(""), fileCount(0), containerCount(0) {}

    Item(eos::ns::ContainerMdProto &&pr, folly::Future<std::string> &&path,
      folly::Future<uint64_t> &&filec, folly::Future<uint64_t> &&containerc)
    : proto(std::move(pr)), fullPath(std::move(path)), fileCount(std::move(filec)),
      containerCount(std::move(containerc)) {}
  };

  //----------------------------------------------------------------------------
  //! Get current element
  //----------------------------------------------------------------------------
  bool getItem(eos::ns::ContainerMdProto &proto, Item *item = nullptr);

  //----------------------------------------------------------------------------
  //! Get number of elements scanned so far
  //----------------------------------------------------------------------------
  uint64_t getScannedSoFar() const;

private:
  //----------------------------------------------------------------------------
  //! Ensure our item deque contanis a sufficient number of pending items
  //----------------------------------------------------------------------------
  void ensureItemDequeFull();

  ContainerScannerPrimitive mScanner;
  qclient::QClient &mQcl;
  bool mFullPaths;
  bool mCounts;
  bool mActive;

  std::deque<Item> mItemDeque;
  uint64_t mScanned = 0;
};



EOSNSNAMESPACE_END