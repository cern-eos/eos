// ----------------------------------------------------------------------
// File: HttpResponse.cc
// Author: Justin Lewis Salmon - CERN
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

/*----------------------------------------------------------------------------*/
#include "common/http/HttpResponse.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
HttpResponse::AddHeader (const std::string key, const std::string value)
{
  mResponseHeaders[key] = value;
}

/*----------------------------------------------------------------------------*/
std::string
HttpResponse::ContentType (const std::string &path)
{
  XrdOucString name = path.c_str();
  if (name.endswith(".txt") || name.endswith(".log"))
    return "text/plain";
  if (name.endswith(".xml"))
    return "text/xml";
  if (name.endswith(".gif"))
    return "image/gif";
  if (name.endswith(".jpg"))
    return "image/jpg";
  if (name.endswith(".png"))
    return "image/png";
  if (name.endswith(".tiff"))
    return "image/tiff";
  if (name.endswith(".mp3"))
    return "audio/mp3";
  if (name.endswith(".mp4"))
    return "audio/mp4";
  if (name.endswith(".pdf"))
    return "application/pdf";
  if (name.endswith(".zip"))
    return "application/zip";
  if (name.endswith(".gzip"))
    return "application/gzip";
  if (name.endswith(".tar.gz"))
    return "application/gzip";
  // default is text/plain
  return "text/plain";
}

/*----------------------------------------------------------------------------*/
std::string
HttpResponse::ToString ()
{
  std::stringstream ss;
  ss <<   "Response code: " << mResponseCode << std::endl;
  for (auto it = GetHeaders().begin(); it != GetHeaders().end(); ++it)
  {
    ss << it->first  << ": " << it->second << std::endl;
  }
  ss << "\n\n" << mResponseBody << std::endl;
  return ss.str();
}

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
