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
//! @brief Class for formatting and printing namespace protobuf objects
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "proto/FileMd.pb.h"
#include "proto/ContainerMd.pb.h"
#include <ostream>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! File printing options
//------------------------------------------------------------------------------
struct FilePrintingOptions {
  bool showId = true;
  bool showContId = true;
  bool showUid = true;
  bool showGid = true;
  bool showSize = true;
  bool showLayoutId = true;
  bool showFlags = true;
  bool showName = true;
  bool showLinkName = true;
  bool showCTime = true;
  bool showMTime = true;
  bool showChecksum = true;
  bool showLocations = true;
  bool showUnlinkLocations = true;
  bool showXAttr = true;
  bool showSTime = true;
};

//------------------------------------------------------------------------------
//! Container printing options
//------------------------------------------------------------------------------
struct ContainerPrintingOptions {
  bool showId = true;
  bool showParent = true;
  bool showUid = true;
  bool showGid = true;
  bool showTreeSize = true;
  bool showMode = true;
  bool showFlags = true;
  bool showName = true;
  bool showCTime = true;
  bool showMTime = true;
  bool showSTime = true;
  bool showXAttr = true;
};

//------------------------------------------------------------------------------
//! Printing class
//------------------------------------------------------------------------------
class Printing
{
public:
  //----------------------------------------------------------------------------
  //! Print the given FileMd protobuf using multiple lines, full information
  //----------------------------------------------------------------------------
  static void printMultiline(const eos::ns::FileMdProto& proto,
                             std::ostream& stream);
  static std::string printMultiline(const eos::ns::FileMdProto& proto);

  //----------------------------------------------------------------------------
  //! Print the given ContainerMd protobuf using multiple lines, full information
  //----------------------------------------------------------------------------
  static void printMultiline(const eos::ns::ContainerMdProto& proto,
                             std::ostream& stream);

  //----------------------------------------------------------------------------
  //! timespec to fileinfo: Convert a timespec into
  //! "Wed Nov 11 15:38:31 2015 Timestamp: 1447252711.38412918"
  //----------------------------------------------------------------------------
  static void timespecToFileinfo(const struct timespec& val,
                                 std::ostream& stream);
  static std::string timespecToFileinfo(const struct timespec& val);
  static std::string timespecToTimestamp(const struct timespec& val);

  //----------------------------------------------------------------------------
  // Escape non-printable string
  //----------------------------------------------------------------------------
  static std::string escapeNonPrintable(const std::string& str);

  //----------------------------------------------------------------------------
  //! Parse timespec
  //----------------------------------------------------------------------------
  template<typename T>
  static struct timespec parseTimespec(const T& bytes)
  {
    struct timespec spec = {};

    if (bytes.length()) {
      (void) memcpy(&spec, bytes.data(), sizeof(struct timespec));
    }

    return spec;
  }


};

EOSNSNAMESPACE_END
