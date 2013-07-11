// ----------------------------------------------------------------------
// File: S3Response.hh
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

/**
 * @file   S3Response.hh
 *
 * @brief  TODO
 */

#ifndef __EOSCOMMON_S3_RESPONSE__HH__
#define __EOSCOMMON_S3_RESPONSE__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/HttpResponse.hh"
#include "common/Namespace.hh"
#include "common/Logging.hh"

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

#define XML_V1_UTF8 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"

class S3Response : public HttpResponse
{

public:

  HttpResponse*
  BuildResponse (HttpRequest *request) { return this; };

  /*----------------------------------------------------------------------------*/
//  static std::string
//  RestErrorResponse (int        &response_code,
//                     int         http_code,
//                     std::string errcode,
//                     std::string errmsg,
//                     std::string resource,
//                     std::string requestid)
//  {
//    //.............................................................................
//    // Creates a AWS RestError Response string
//    //.............................................................................
//    response_code = http_code;
//    std::string result = XML_V1_UTF8;
//    result += "<Error><Code>";
//    result += errcode;
//    result += "</Code>";
//    result += "<Message>";
//    result += errmsg;
//    result += "</Message>";
//    result += "<Resource>";
//    result += resource;
//    result += "</Resource>";
//    result += "<RequestId>";
//    result += requestid;
//    result += "</RequestId>";
//    result += "</Error";
//    return result;
//  }
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif /* __EOSCOMMON_S3_RESPONSE__HH__ */
