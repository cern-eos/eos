// ----------------------------------------------------------------------
// File: URLBuilder.cc
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

#include "URLBuilder.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include <unistd.h>

EOSMGMRESTNAMESPACE_BEGIN

std::unique_ptr<URLBuilderProtocol> URLBuilder::getInstance()
{
  std::unique_ptr<URLBuilderProtocol> ret;
  ret.reset(new URLBuilder());
  //copy-ellision here
  return ret;
}

URLBuilderHostname* URLBuilder::setHttpsProtocol()
{
  mURL = "https://";
  return this;
}

URLBuilderPort* URLBuilder::setHostname(const std::string& hostname)
{
  mURL += hostname;
  return this;
}

URLBuilder* URLBuilder::setPort(const uint16_t& port)
{
  mURL += ":" + std::to_string(port);
  return this;
}

URLBuilder* URLBuilder::add(const std::string& urlItem)
{
  addSlashIfNecessary(urlItem);
  mURL += urlItem;
  return this;
}

std::string URLBuilder::build() const
{
  return mURL;
}

void URLBuilder::addSlashIfNecessary(const std::string& nextItem)
{
  if (mURL.back() != '/' && (!nextItem.empty() && nextItem.front() != '/')) {
    mURL += "/";
  }
}

EOSMGMRESTNAMESPACE_END
