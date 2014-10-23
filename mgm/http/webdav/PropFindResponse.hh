// ----------------------------------------------------------------------
// File: PropFindResponse.hh
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
 * @file   PropFindResponse.hh
 *
 * @brief  Class responsible for parsing a WebDAV PROPFIND request and
 *         building a response.
 */

#ifndef __EOSMGM_PROPFIND_RESPONSE__HH__
#define __EOSMGM_PROPFIND_RESPONSE__HH__

#define EOS_WEBDAV_HIDE_IN_PROPFIND_PREFIX ".sys.dav.hide#."

/*----------------------------------------------------------------------------*/
#include "mgm/http/webdav/WebDAVResponse.hh"
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
// rapidjason header file violates ordered prototype delcaration
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 6)
// =============================================================================
#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-fpermissive"
#include "mgm/http/rapidxml/rapidxml.hpp"
#include "mgm/http/rapidxml/rapidxml_print.hpp"
#pragma GCC diagnostic pop
// =============================================================================
#else
// =============================================================================
#pragma GCC diagnostic warning "-fpermissive"
#include "mgm/http/rapidxml/rapidxml.hpp"
#include "mgm/http/rapidxml/rapidxml_print.hpp"
// =============================================================================
#endif

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN;


//
extern char dav_rfc3986[256];
extern char dav_html5[256];

/**
 * URI decoding routine
 */
extern int dav_uri_decode (char* source, char* dest );

class PropFindResponse : public WebDAVResponse {
public:

  /**
   * PROPFIND available property types
   */
  enum PropertyTypes {
    NONE = 0x0000,
    CREATION_DATE = 0x0001,
    GET_CONTENT_LENGTH = 0x0002,
    GET_LAST_MODIFIED = 0x0004,
    RESOURCE_TYPE = 0x0008,
    CHECKED_IN = 0x0010,
    CHECKED_OUT = 0x0020,
    DISPLAY_NAME = 0x0040,
    GET_CONTENT_TYPE = 0x0080,
    GET_ETAG = 0x0100,
    QUOTA_AVAIL = 0x0200,
    QUOTA_USED = 0x0400,
    GET_OCID = 0x0800
  };

protected:
  int mRequestPropertyTypes; //!< properties that were requested
  eos::common::Mapping::VirtualIdentity *mVirtualIdentity; //!< virtual identity for this client

public:

  /**
   * Constructor
   *
   * @param request  the client request object
   */
  PropFindResponse (eos::common::HttpRequest *request,
                    eos::common::Mapping::VirtualIdentity *vid) :
  WebDAVResponse (request), mRequestPropertyTypes (NONE),
  mVirtualIdentity (vid)
  {
    static bool initialized = false;
    if (!initialized)
    {
      // initialize encoding table
      for (int i = 0; i < 256; i++)
      {
        dav_rfc3986[i] = isalnum(i) || i == '-' || i == '.' || i == '_'
          || i == '~' || i == '/'
          ? i : 0;
        dav_html5[i] = isalnum(i) || i == '*' || i == '-' || i == '.' || i == '_'
          ? i : (i == ' ') ? '+' : 0;
      }
    }
  };

  /**
   * Destructor
   */
  virtual ~PropFindResponse ()
  {
  };


  /**
   * Build an appropriate response to the given PROPFIND request.
   *
   * @param request  the client request object
   *
   * @return the newly built response object
   */
  HttpResponse*
  BuildResponse (eos::common::HttpRequest *request);

  /**
   * Check the request XML to find out which properties were requested and
   * will therefore need to be returned.
   *
   * @param node  the root node of the PROPFIND request body
   */
  void
  ParseRequestPropertyTypes (rapidxml::xml_node<> *node);

  /**
   * Build a response XML <response/> node containing the properties that were
   * requested, whether they were found or not, etc (see RFC)
   *
   * @param url  the URL of the resource to build a response node for
   *
   * @return the newly build response node
   */
  rapidxml::xml_node<>*
  BuildResponseNode (const std::string &url, const std::string &hrefurl);

  /**
   * Convert the given property type string into its integer constant
   * representation.
   *
   * @param property  the property type string to convert
   *
   * @return the converted property string as an integer
   */
  inline PropertyTypes
  MapRequestPropertyType (std::string property)
  {
    if (property == "getcontentlength") return GET_CONTENT_LENGTH;
    else if (property == "getcontenttype") return GET_CONTENT_TYPE;
    else if (property == "getlastmodified") return GET_LAST_MODIFIED;
    else if (property == "getetag") return GET_ETAG;
    else if (property == "displayname") return DISPLAY_NAME;
    else if (property == "creationdate") return CREATION_DATE;
    else if (property == "resourcetype") return RESOURCE_TYPE;
    else if (property == "checked-in") return CHECKED_IN;
    else if (property == "checked-out") return CHECKED_OUT;
    else if (property == "quota-available-bytes") return QUOTA_AVAIL;
    else if (property == "quota-used-bytes") return QUOTA_USED;
    else if (property == "id") return GET_OCID;
    else return NONE;
  }

  /**
   * Encode an URI
   *
   * @param path is the URI to encode
   *
   * @return an sdt::string with the encoded URI
   */
  std::string
  EncodeURI (const char* uri);
};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_PROPFIND_RESPONSE__HH__ */
