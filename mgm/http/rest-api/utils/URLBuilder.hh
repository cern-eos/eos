// ----------------------------------------------------------------------
// File: URLBuilder.hh
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#ifndef EOS_URLBUILDER_HH
#define EOS_URLBUILDER_HH

#include "mgm/Namespace.hh"

#include <cstdint>
#include <string>
#include <memory>
#include <optional>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class allows to build a URL by using the pattern fluent builder.
 * The programmer will create the URL following the order imposed by the builder
 */
class URLBuilder;

class URLBuilderPort
{
public:
  virtual URLBuilder* setPort(const uint16_t& port) = 0;
  virtual URLBuilder* add(const std::string& urlItem) = 0;
  virtual ~URLBuilderPort() = default;
};

class URLBuilderHostname
{
public:
  virtual URLBuilderPort* setHostname(const std::string& hostname) = 0;
  virtual ~URLBuilderHostname() = default;
};

class URLBuilderProtocol
{
public:
  virtual URLBuilderHostname* setHttpsProtocol() = 0;
  virtual ~URLBuilderProtocol() = default;
};

/**
 * Actual URL builder
 */
class URLBuilder : public URLBuilderProtocol, URLBuilderHostname,
  URLBuilderPort
{
public:
  /**
   * Returns the URL built with the builder
   * @return the URL built with the builder
   */
  std::string build() const;
  URLBuilder* add(const std::string& urlItem) override;

  /**
   * Get the instance of the builder. It will first return the Builder allowing to generate the protocol of the URL
   * @return the pointer to the instance of the builder allowing to generate the protocol of the URL
   */
  static std::unique_ptr<URLBuilderProtocol> getInstance();
private:
  URLBuilder() = default;
  /**
   * Adds the https:// string to the URL
   * @return the builder to add the hostname
   */
  URLBuilderHostname* setHttpsProtocol() override;
  /**
   * Generates and add the builder
   * @return
   */
  URLBuilderPort* setHostname(const std::string& hostname) override;
  URLBuilder* setPort(const uint16_t& port) override;
  std::string mURL;
  void addSlashIfNecessary(const std::string& nextItem = "");
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_URLBUILDER_HH
