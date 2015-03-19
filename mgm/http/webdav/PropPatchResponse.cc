// ----------------------------------------------------------------------
// File: PropPatchResponse.cc
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
#include "mgm/http/webdav/PropPatchResponse.hh"
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
PropPatchResponse::BuildResponse (eos::common::HttpRequest *request)
{
  using namespace rapidxml;

  // Get the namespaces (if any)
  ParseNamespaces();

  eos_static_debug("\n%s", request->GetBody().c_str());

  // Root node <propertyupdate/>
  xml_node<> *updateNode = mXMLRequestDocument.first_node();
  if (!updateNode)
  {
    SetResponseCode(ResponseCodes::BAD_REQUEST);
    return this;
  }

  // Build the response
  // xml declaration
  xml_node<> *decl = mXMLResponseDocument.allocate_node(node_declaration);
  decl->append_attribute(AllocateAttribute("version", "1.0"));
  decl->append_attribute(AllocateAttribute("encoding", "utf-8"));
  mXMLResponseDocument.append_node(decl);

  // <multistatus/> node
  xml_node<> *multistatusNode = AllocateNode("d:multistatus");
  multistatusNode->append_attribute(AllocateAttribute("xmlns:d", "DAV:"));

  // add custom namespaces
  NamespaceMap::const_iterator it;
  for (it = mCustomNamespaces.begin(); it != mCustomNamespaces.end(); ++it)
  {
    std::string ns = "xmlns:";
    ns += it->first;
    multistatusNode->append_attribute(AllocateAttribute(ns.c_str(), it->second.c_str()));
  }

  mXMLResponseDocument.append_node(multistatusNode);

  // <d:response/> node
  xml_node<> *responseNode = AllocateNode("d:response");
  multistatusNode->append_node(responseNode);

  // <d:href/> node
  xml_node<> *hrefNode = AllocateNode("d:href");
  responseNode->append_node(hrefNode);

  // now get all the set/remove nodes and send a fake OK for each of them
  xml_node<> *setNode = GetNode(updateNode, "set");
  xml_node<> *removeNode = GetNode(updateNode, "remove");

  if (setNode)
  {
    xml_node<> *propNode = GetNode(setNode, "prop");
    if (propNode)
    {
      xml_node<> *prop = propNode->first_node();
      while (prop)
      {
        xml_node<> *propStat = AllocateNode("d:propstat");
        responseNode->append_node(propStat);

        xml_node<> *propResponse = AllocateNode("d:prop");
        propStat->append_node(propResponse);

        xml_node<> *propKey = AllocateNode(prop->name());
        propResponse->append_node(propKey);

        xml_node<> *status = AllocateNode("d:status");
        SetValue(status, "HTTP/1.1 200 OK");
        propStat->append_node(status);
        prop = prop->next_sibling();
      }
    }
  }

  if (removeNode)
  {
    xml_node<> *propNode = GetNode(setNode, "prop");
    if (propNode)
    {
      xml_node<> *prop = propNode->first_node();
      while (prop)
      {
        xml_node<> *propStat = AllocateNode("d:propstat");
        responseNode->append_node(propStat);

        xml_node<> *propResponse = AllocateNode("d:prop");
        propStat->append_node(propResponse);

        xml_node<> *propKey = AllocateNode(prop->name());
        propResponse->append_node(propKey);

        xml_node<> *status = AllocateNode("d:status");
        SetValue(status, "HTTP/1.1 200 OK");
        propStat->append_node(status);
        prop = prop->next_sibling();
      }
    }
  }

  std::string responseString;
  rapidxml::print(std::back_inserter(responseString), mXMLResponseDocument, rapidxml::print_no_indenting);
  mXMLResponseDocument.clear();

  SetResponseCode(HttpResponse::MULTI_STATUS);

  AddHeader("Content-Length", std::to_string((long long) responseString.size()));
  AddHeader("Content-Type", "application/xml; charset=utf-8");
  SetBody(responseString);
  return this;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
