// ----------------------------------------------------------------------
// File: WebDAVResponse.hh
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
 * @file   WebDAVResponse.hh
 *
 * @brief  Abstract WebDAV response class. Stores XML request/response
 *         information and contains useful functions for building XML documents
 *         using RapidXML.
 */

#ifndef __EOSMGM_WEBDAV_RESPONSE__HH__
#define __EOSMGM_WEBDAV_RESPONSE__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/HttpResponse.hh"
#include "common/http/HttpRequest.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include "mgm/http/rapidxml/rapidxml.hpp"
#include <vector>
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

class WebDAVResponse : public eos::common::HttpResponse
{

public:
  typedef std::map<std::string, std::string> NamespaceMap;

protected:
  rapidxml::xml_document<> mXMLRequestDocument;  //!< the parsed XML request
  rapidxml::xml_document<> mXMLResponseDocument; //!< the XML response
  std::vector<char>        mXMLRequestCopy;      //!< modifiable request copy
  NamespaceMap             mDAVNamespaces;       //!< all DAV: namespaces
  NamespaceMap             mCustomNamespaces;    //!< all custom namespaces

public:

  /**
   * Constructor
   *
   * @param request  the client request object
   */
  WebDAVResponse (eos::common::HttpRequest *request);

  /**
   * Destructor
   */
  virtual ~WebDAVResponse () {};

  /**
   * Build an appropriate response to the given WebDAV request. This will be
   * implemented by each specific WebDAV request type (e.g. PROPFIND, COPY).
   *
   * @param request  the client request object
   *
   * @return the newly built response object
   */
  virtual eos::common::HttpResponse*
  BuildResponse (eos::common::HttpRequest *request) = 0;

  /**
   * Scan through the request XML document looking for any DAV: or custom
   * namespace declarations.
   */
  void
  ParseNamespaces ();

  /**
   * Find a sub node of the given node (not recursively).
   *
   * @param node  the node whose children to search
   * @param name  the name of the child node to search for
   *
   * @return the newly found child node, or NULL if not found
   */
  rapidxml::xml_node<>*
  GetNode (rapidxml::xml_node<> *node, const char *name);

  /**
   * Add a node to the response XML document by using the RapidXML memory
   * pool.
   *
   * @param name  the name of the new node to be allocated
   *
   * @return a pointer to the newly allocated node
   */
  rapidxml::xml_node<>*
  AllocateNode (const char *name);

  /**
   * Add an attribute to the response XML document by using the RapidXML memory
   * pool.
   *
   * @param name   the name of the new attribute
   * @param value  the value of the new attribute
   *
   * @return a pointer to the newly allocated attribute
   */
  rapidxml::xml_attribute<>*
  AllocateAttribute (const char *name, const char *value);

  /**
   * Add a string to the response XML document memory pool.
   *
   * @param value  the string to be allocated
   *
   * @return a pointer inside the XML document to the newly allocated string
   */
  const char*
  AllocateString (const char *value);

  /**
   * Set the text contents of the given node, making sure the string is
   * properly allocated inside the RapidXML memory pool.
   *
   * @param node   pointer to the node which needs a value
   * @param value  the value to be set
   */
  void
  SetValue (rapidxml::xml_node<> *node, const char *value);

};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif /* __EOSMGM_WEBDAV_RESPONSE__HH__ */
