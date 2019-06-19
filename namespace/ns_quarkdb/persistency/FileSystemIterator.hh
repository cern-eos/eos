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
//! @brief Class to iterate through all available FileSystems, as found in
//!        the namespace
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include <qclient/structures/QScanner.hh>

namespace qclient {
  class QClient;
}

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! FileSystemIterator class
//------------------------------------------------------------------------------
class FileSystemIterator {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileSystemIterator(qclient::QClient &qcl);

  //----------------------------------------------------------------------------
  //! Get next filesystem ID
  //----------------------------------------------------------------------------
  IFileMD::location_t getFileSystemID() const;

  //----------------------------------------------------------------------------
  //! Is this referring to the unlinked view?
  //----------------------------------------------------------------------------
  bool isUnlinked() const;

  //----------------------------------------------------------------------------
  //! What is the raw redis key this element is referring to?
  //----------------------------------------------------------------------------
  std::string getRedisKey() const;

  //----------------------------------------------------------------------------
  //! Is the iterator object valid?
  //----------------------------------------------------------------------------
  bool valid() const;

  //----------------------------------------------------------------------------
  //! Advance iterator
  //----------------------------------------------------------------------------
  void next();

private:
  //----------------------------------------------------------------------------
  //! Parse element that mScanner currently points to - return false on parse
  //! error
  //----------------------------------------------------------------------------
  bool parseScannerKey();
  bool rawParseScannerKey();

  qclient::QScanner mScanner;

  std::string mRedisKey;
  IFileMD::location_t mFilesystemID;
  bool mIsUnlinked;
};

EOSNSNAMESPACE_END
