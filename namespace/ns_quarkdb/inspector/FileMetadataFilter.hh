/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
//! @brief Class to filter out FileMDs
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "proto/FileMd.pb.h"
#include "common/Status.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class to decide whether to show a particular FileMD
//------------------------------------------------------------------------------
class FileMetadataFilter {
public:
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileMetadataFilter() {}

  //----------------------------------------------------------------------------
  //! Is the object valid?
  //----------------------------------------------------------------------------
  virtual common::Status isValid() const = 0;

  //----------------------------------------------------------------------------
  //! Does the given FileMdProto pass through the filter?
  //----------------------------------------------------------------------------
  virtual bool check(const eos::ns::FileMdProto &proto) = 0;

  //----------------------------------------------------------------------------
  //! Describe object
  //----------------------------------------------------------------------------
  virtual std::string describe() const = 0;

};

//------------------------------------------------------------------------------
//! Filter which checks a particular FileMdProto attribute for equality
//------------------------------------------------------------------------------
class EqualityFileMetadataFilter : public FileMetadataFilter {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  EqualityFileMetadataFilter(const std::string &attr, const std::string &value);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~EqualityFileMetadataFilter() {}

  //----------------------------------------------------------------------------
  //! Is the object valid?
  //----------------------------------------------------------------------------
  virtual common::Status isValid() const override;

  //----------------------------------------------------------------------------
  //! Does the given FileMdProto pass through the filter?
  //----------------------------------------------------------------------------
  virtual bool check(const eos::ns::FileMdProto &proto) override;

  //----------------------------------------------------------------------------
  //! Describe object
  //----------------------------------------------------------------------------
  virtual std::string describe() const override;

private:
  std::string mAttr;
  std::string mValue;
};

EOSNSNAMESPACE_END
