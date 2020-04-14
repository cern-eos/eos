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
HttpResponse::AddHeader(const std::string key, const std::string value)
{
  mResponseHeaders[key] = value;
}

/*----------------------------------------------------------------------------*/
std::string
HttpResponse::ContentType(const std::string& path)
{
  XrdOucString name = path.c_str();

  if (name.endswith(".txt") || name.endswith(".log")) {
    return "text/plain";
  }

  if (name.endswith(".xml")) {
    return "text/xml";
  }

  if (name.endswith(".gif")) {
    return "image/gif";
  }

  if (name.endswith(".jpg")) {
    return "image/jpg";
  }

  if (name.endswith(".png")) {
    return "image/png";
  }

  if (name.endswith(".tiff")) {
    return "image/tiff";
  }

  if (name.endswith(".mp3")) {
    return "audio/mp3";
  }

  if (name.endswith(".mp4")) {
    return "audio/mp4";
  }

  if (name.endswith(".pdf")) {
    return "application/pdf";
  }

  if (name.endswith(".zip")) {
    return "application/zip";
  }

  if (name.endswith(".gzip")) {
    return "application/gzip";
  }

  if (name.endswith(".tar.gz")) {
    return "application/gzip";
  }

  // default is text/plain
  return "text/plain";
}

/*----------------------------------------------------------------------------*/
std::string
HttpResponse::ToString()
{
  std::stringstream ss;
  ss <<   "Response code: " << mResponseCode << std::endl;

  for (auto it = GetHeaders().begin(); it != GetHeaders().end(); ++it) {
    ss << it->first  << ": " << it->second << std::endl;
  }

  ss << "\n\n" << mResponseBody << std::endl;
  return ss.str();
}

/*----------------------------------------------------------------------------*/
std::string
HttpResponse::GetResponseCodeDescription()
{
  switch (mResponseCode) {
  case CONTINUE:
    return std::string("CONTINUE");

  case OK:
    return std::string("OK");

  case CREATED:
    return std::string("CREATED");

  case NO_CONTENT:
    return std::string("NO_CONTENT");

  case PARTIAL_CONTENT:
    return std::string("PARTIAL_CONTENT");

  case MULTI_STATUS:
    return std::string("MULTI_STATUS");

  case NOT_MODIFIED:
    return std::string("NOT_MODIFIED");

  case TEMPORARY_REDIRECT:
    return std::string("TEMPORARY_REDIRECT");

  case BAD_REQUEST:
    return std::string("BAD_REQUEST");

  case UNAUTHORIZED:
    return std::string("UNAUTHORIZED");

  case FORBIDDEN:
    return std::string("FORBIDDEN");

  case NOT_FOUND:
    return std::string("NOT_FOUND");

  case METHOD_NOT_ALLOWED:
    return std::string("METHOD_NOT_ALLOWED");

  case CONFLICT:
    return std::string("CONFLICT");

  case LENGTH_REQUIRED:
    return std::string("LENGTH_REQUIRED");

  case PRECONDITION_FAILED:
    return std::string("PRECONDITION_FAILED");

  case UNSUPPORTED_MEDIA_TYPE:
    return std::string("UNSUPPORTED_MEDIA_TYPE");

  case REQUESTED_RANGE_NOT_SATISFIABLE:
    return std::string("REQUESTED_RANGE_NOT_SATISFIABLE");

  case UNPROCESSABLE_ENTITY:
    return std::string("UNPROCESSABLE_ENTITY");

  case INTERNAL_SERVER_ERROR:
    return std::string("INTERNAL_SERVER_ERROR");

  case NOT_IMPLEMENTED:
    return std::string("NOT_IMPLEMENTED");

  case BAD_GATEWAY:
    return std::string("BAD_GATEWAY");

  case SERVICE_UNAVAILABLE:
    return std::string("SERVICE_UNAVAILABLE");

  default:
    return std::string("UNKNOWN_RESPONSE_CODE");
  }
}


EOSCOMMONNAMESPACE_END
