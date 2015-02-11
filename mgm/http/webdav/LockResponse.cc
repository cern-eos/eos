// ----------------------------------------------------------------------
// File: LockResponse.cc
// Author: Andreas-Joachim Peters - CERN
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
#include "mgm/http/webdav/LockResponse.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/Quota.hh"
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "common/Path.hh"
#include "common/http/OwnCloud.hh"

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucErrInfo.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
LockResponse::BuildResponse (eos::common::HttpRequest *request)
{
  using namespace rapidxml;

  // Get the namespaces (if any)
  ParseNamespaces();

  eos_static_debug("\n%s", request->GetBody().c_str());
  // Root node <lockinfo/>
  xml_node<> *infoNode = mXMLRequestDocument.first_node();
  if (!infoNode)
  {
    SetResponseCode(ResponseCodes::BAD_REQUEST);
    return this;
  }
  xml_node<> *property = infoNode->first_node();
  
  // Build the response
  // xml declaration
  xml_node<> *decl = mXMLResponseDocument.allocate_node(node_declaration);
  decl->append_attribute(AllocateAttribute("version", "1.0"));
  decl->append_attribute(AllocateAttribute("encoding", "utf-8"));
  mXMLResponseDocument.append_node(decl);

  // <prop/> node
  xml_node<> *propNode = AllocateNode("prop");
  propNode->append_attribute(AllocateAttribute("xmlns", "DAV:"));
  mXMLResponseDocument.append_node(propNode);

  // <lockdiscovery/> node
  xml_node<> *lockdiscoveryNode = AllocateNode("lockdiscovery");
  propNode->append_node(lockdiscoveryNode);

  // <activelock/> node
  xml_node<> *activelockNode = AllocateNode("activelock");
  lockdiscoveryNode->append_node(activelockNode);

  // Find all the request properties
  while (property) {
    XrdOucString propertyName = property->name();
    eos_static_debug("msg=\"found xml property: %s\" value=\"%s\"", propertyName.c_str(), property->value());
    xml_node<> *cloned_node = CloneNode(property);
    activelockNode->append_node(cloned_node);
    property = property->next_sibling();
  }

  // <timeout/> node
  xml_node<> *timeoutNode = AllocateNode("timeout");
  SetValue(timeoutNode, "Second-604800");
  activelockNode->append_node(timeoutNode);

  // <depth/> node
  xml_node<> *depthNode = AllocateNode("depth");
  SetValue(depthNode, "Infinity");
  activelockNode->append_node(depthNode);

  // <locktoken/> node
  xml_node<> *locktokenNode = AllocateNode("locktoken");
  activelockNode->append_node(locktokenNode);
  
  // <href/> node
  xml_node<> *hrefNode = AllocateNode("href");
  SetValue(hrefNode,"opaquelocktoken:00000000-0000-0000-0000-000000000000");
  locktokenNode->append_node(hrefNode);
  
  std::string responseString;
  rapidxml::print(std::back_inserter(responseString), mXMLResponseDocument, rapidxml::print_no_indenting);
  mXMLResponseDocument.clear();

  AddHeader("Content-Length", std::to_string((long long) responseString.size()));
  AddHeader("Content-Type", "application/xml; charset=utf-8");
  AddHeader("Lock-Token", "opaquelocktoken:00000000-0000-0000-0000-000000000000");
  SetBody(responseString);
  return this;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
