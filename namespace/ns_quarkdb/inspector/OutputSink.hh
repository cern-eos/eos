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
#include "namespace/ns_quarkdb/inspector/FileScanner.hh"
#include "namespace/ns_quarkdb/inspector/ContainerScanner.hh"
#include "namespace/ns_quarkdb/inspector/Printing.hh"
#include "proto/ContainerMd.pb.h"
#include "proto/FileMd.pb.h"
#include <json/json.h>
#include <map>

EOSNSNAMESPACE_BEGIN

struct ContainerPrintingOptions;
struct FilePrintingOptions;

//------------------------------------------------------------------------------
//! Interface for printing output
//------------------------------------------------------------------------------
class OutputSink
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  OutputSink(std::ostream& out, std::ostream& err):
    mOut(out), mErr(err)
  {}

  //----------------------------------------------------------------------------
  //! Virtual destructor
  //----------------------------------------------------------------------------
  virtual ~OutputSink() = default;

  //----------------------------------------------------------------------------
  //! Print interface
  //----------------------------------------------------------------------------
  virtual void print(const std::map<std::string, std::string>& out) = 0;

  //----------------------------------------------------------------------------
  //! Print interface for string
  //----------------------------------------------------------------------------
  virtual void print(const std::string& out)
  {
    mOut << Printing::escapeNonPrintable(out) << std::endl;
  }

  //----------------------------------------------------------------------------
  //! Print interface for json object
  //----------------------------------------------------------------------------
  virtual void print(const Json::Value& json_obj)
  {
    mOut << json_obj << std::endl;
  }

  //----------------------------------------------------------------------------
  //! Debug output
  //----------------------------------------------------------------------------
  virtual void err(const std::string& str)
  {
    mErr << Printing::escapeNonPrintable(str) << std::endl;
  }

  //----------------------------------------------------------------------------
  //! Print everything known about a ContainerMD
  //----------------------------------------------------------------------------
  void print(const eos::ns::ContainerMdProto& proto,
             const ContainerPrintingOptions& opts);

  //----------------------------------------------------------------------------
  //! Print everything known about a ContainerMD -- custom path
  //----------------------------------------------------------------------------
  void printWithCustomPath(const eos::ns::ContainerMdProto& proto,
                           const ContainerPrintingOptions& opts,
                           const std::string& customPath);

  //----------------------------------------------------------------------------
  //! Print everything known about a ContainerMD, including
  //! full path if available
  //----------------------------------------------------------------------------
  void print(const eos::ns::ContainerMdProto& proto,
             const ContainerPrintingOptions& opts,
             ContainerScanner::Item& item, bool showCounts);

  //----------------------------------------------------------------------------
  //! Print everything known about a FileMD
  //----------------------------------------------------------------------------
  void print(const eos::ns::FileMdProto& proto, const FilePrintingOptions& opts);

  //----------------------------------------------------------------------------
  //! Print everything known about a FileMD -- custom path
  //----------------------------------------------------------------------------
  void printWithCustomPath(const eos::ns::FileMdProto& proto,
                           const FilePrintingOptions& opts,
                           const std::string& customPath);

  //----------------------------------------------------------------------------
  //! Print everything known about a FileMD -- custom path
  //----------------------------------------------------------------------------
  void printWithAdditionalFields(const eos::ns::FileMdProto& proto,
                                 const FilePrintingOptions& opts,
                                 std::map<std::string, std::string>& extension);
  //----------------------------------------------------------------------------
  //! Print everything known about a FileMD, including full path if available
  //----------------------------------------------------------------------------
  void print(const eos::ns::FileMdProto& proto, const FilePrintingOptions& opts,
             FileScanner::Item& item);

protected:
  std::ostream& mOut;
  std::ostream& mErr;
};

//------------------------------------------------------------------------------
//! OutputSink implementation based on streams
//------------------------------------------------------------------------------
class StreamSink : public OutputSink
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  StreamSink(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Print implementation
  //----------------------------------------------------------------------------
  virtual void print(const std::map<std::string, std::string>& line) override;
};

//------------------------------------------------------------------------------
//! OutputSink implementation based on json streams
//------------------------------------------------------------------------------
class JsonStreamSink : public OutputSink
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  JsonStreamSink(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~JsonStreamSink();

  //----------------------------------------------------------------------------
  //! Print implementation
  //----------------------------------------------------------------------------
  virtual void print(const std::map<std::string, std::string>& line) override;

private:
  bool mFirst;
};


class JsonLinedStreamSink : public OutputSink
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  JsonLinedStreamSink(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Print implementation
  //----------------------------------------------------------------------------
  virtual void print(const std::map<std::string, std::string>& line) override;

  //----------------------------------------------------------------------------
  //! Print interface, single string implementation
  //----------------------------------------------------------------------------
  virtual void print(const Json::Value& jsonObj) override;

private:
  Json::StreamWriterBuilder mBuilder;
  std::unique_ptr<Json::StreamWriter> mWriter;
};


EOSNSNAMESPACE_END
