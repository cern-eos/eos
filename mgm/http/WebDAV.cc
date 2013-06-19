// ----------------------------------------------------------------------
// File: WebDAV.cc
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
#include "mgm/http/WebDAV.hh"
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include "mgm/http/rapidxml/rapidxml.hpp"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
WebDAV::Matches (const std::string &meth, HeaderMap &headers)
{
  int method =  ParseMethodString(meth);
  if (method == PROPFIND || method == PROPPATCH || method == MKCOL ||
      method == COPY     || method == MOVE      || method == LOCK  ||
      method == UNLOCK)
  {
    eos_static_info("info=Matched WebDAV protocol for request");
    return true;
  }
  else return false;
}

/*----------------------------------------------------------------------------*/
void
WebDAV::HandleRequest (HeaderMap         &request,
                       const std::string &method,
                       const std::string &url,
                       const std::string &query,
                       const std::string &body,
                       size_t            *bodysize,
                       HeaderMap         &cookies)
{
  eos_static_info("msg=\"handling webdav request\"");
  std::string result;
  int meth = ParseMethodString(method);

  switch (meth)
  {
  case PROPFIND:
    PropFind(request, body);
    break;
  case PROPPATCH:
    break;
  case MKCOL:
    break;
  case COPY:
    break;
  case MOVE:
    break;
  case LOCK:
    break;
  case UNLOCK:
    break;
  default:
    mResponseCode = 400;
    mResponseBody = "No such method";
  }
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::PropFind (HeaderMap &request, const std::string &body)
{

  using namespace rapidxml;
  xml_document<> doc;

  eos_static_info("body=\n%s", body.c_str());

  // make a safe-to-modify copy of input_xml
  std::vector<char> xml_copy(body.begin(), body.end());
  xml_copy.push_back('\0');

//  for (auto it = xml_copy.begin(); it != xml_copy.end(); ++it) {
//    eos_static_info("char=%s", *it);
//  }

  doc.parse<0>(&xml_copy[0]);

  std::cout << "Name of my first node is: " << doc.first_node()->name() << "\n";
  xml_node<> *node = doc.first_node("propfind");
  std::cout << "Node propfind has value " << node->value() << "\n";
  for (xml_attribute<> *attr = node->first_attribute();
       attr; attr = attr->next_attribute())
  {
    std::cout << "Node propfind has attribute " << attr->name() << " ";
    std::cout << "with value " << attr->value() << "\n";
  }

  std::string resp = "<D:multistatus xmlns:D=\"DAV:\">\n\
  <D:response xmlns:lp1=\"DAV:\" xmlns:g0=\"DAV:\">\n\
    <D:href>/eos/dev/http/</D:href>\n\
    <D:propstat>\n\
      <D:prop>\n\
        <lp1:resourcetype>\n\
          <D:collection/>\n\
        </lp1:resourcetype>\n\
        <lp1:getlastmodified>Mon, 25 Jul 2011 08:49:40 GMT</lp1:getlastmodified>\n\
        <lp1:creationdate>2011-07-25T08:49:40Z</lp1:creationdate>\n\
      </D:prop>\n\
      <D:status>HTTP/1.1 200 OK</D:status>\n\
    </D:propstat>\n\
    <D:propstat>\n\
      <D:prop>\n\
        <g0:getcontentlength/>\n\
        <executable xmlns=\"http://apache.org/dav/props/\"/>\n\
        <resourcetype xmlns=\"DAV:\"/>\n\
        <checked-in xmlns=\"DAV:\"/>\n\
        <checked-out xmlns=\"DAV:\"/>\n\
      </D:prop>\n\
      <D:status>HTTP/1.1 404 Not Found</D:status>\n\
    </D:propstat>\n\
  </D:response>\n\
</D:multistatus>\n";

  mResponseCode = 207;
  mResponseHeaders["Content-Length"] = std::to_string((long long) resp.size());
  mResponseHeaders["Content-Type"] = "text/xml; charset=\"utf-8\"";
  mResponseBody = resp;

  return "";
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::PropPatch (HeaderMap &request, HeaderMap &response, int &respcode)
{
  return "";
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::MkCol (HeaderMap &request, HeaderMap &response, int &respcode)
{
  return "";
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::Copy (HeaderMap &request, HeaderMap &response, int &respcode)
{
  return "";
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::Move (HeaderMap &request, HeaderMap &response, int &respcode)
{
  return "";
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::Lock (HeaderMap &request, HeaderMap &response, int &respcode)
{
  return "";
}

/*----------------------------------------------------------------------------*/
std::string
WebDAV::Unlock (HeaderMap &request, HeaderMap &response, int &respcode)
{
  return "";
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
