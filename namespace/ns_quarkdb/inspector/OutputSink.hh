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
//! @brief Abstract class to send / format output
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "proto/ContainerMd.pb.h"
#include <map>

EOSNSNAMESPACE_BEGIN

struct ContainerPrintingOptions;

//------------------------------------------------------------------------------
//! Interface for printing output
//------------------------------------------------------------------------------
class OutputSink {
public:
  //----------------------------------------------------------------------------
  //! Virtual destructor
  //----------------------------------------------------------------------------
  virtual ~OutputSink() {}

  //----------------------------------------------------------------------------
  //! Print interface
  //----------------------------------------------------------------------------
  virtual void print(const std::map<std::string, std::string> &out) = 0;

  //----------------------------------------------------------------------------
  //! Print everything known about a ContainerMD
  //----------------------------------------------------------------------------
  void print(const eos::ns::ContainerMdProto &proto, const ContainerPrintingOptions &opts);

  //----------------------------------------------------------------------------
  //! Debug output
  //----------------------------------------------------------------------------
  virtual void err(const std::string &str) = 0;

private:

};

//------------------------------------------------------------------------------
//! OutputSink implementation based on streams
//------------------------------------------------------------------------------
class StreamSink : public OutputSink {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  StreamSink(std::ostream &out, std::ostream &err);

  //----------------------------------------------------------------------------
  //! Print implementation
  //----------------------------------------------------------------------------
  virtual void print(const std::map<std::string, std::string> &line) override;

  //----------------------------------------------------------------------------
  //! Debug output
  //----------------------------------------------------------------------------
  virtual void err(const std::string &str) override;

private:
  std::ostream &mOut;
  std::ostream &mErr;
};

//------------------------------------------------------------------------------
//! OutputSink implementation based on json streams
//------------------------------------------------------------------------------
class JsonStreamSink : public OutputSink {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  JsonStreamSink(std::ostream &out, std::ostream &err);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~JsonStreamSink();

  //----------------------------------------------------------------------------
  //! Print implementation
  //----------------------------------------------------------------------------
  virtual void print(const std::map<std::string, std::string> &line) override;

  //----------------------------------------------------------------------------
  //! Debug output
  //----------------------------------------------------------------------------
  virtual void err(const std::string &str) override;

private:
  std::ostream &mOut;
  std::ostream &mErr;

  bool mFirst;
};


EOSNSNAMESPACE_END
