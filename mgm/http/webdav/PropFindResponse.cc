// ----------------------------------------------------------------------
// File: PropFindResponse.cc
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
#include "mgm/http/webdav/PropFindResponse.hh"
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
char dav_rfc3986[256] = {0};
char dav_html5[256] = {0};

/*----------------------------------------------------------------------------*/
void
dav_uri_encode (unsigned char *s, char *enc, char *tb)
{
  for (; *s; s++)
  {
    if (tb[*s]) sprintf(enc, "%c", tb[*s]);
    else sprintf(enc, "%%%02X", *s);
    while (*++enc);
  }
}

int
dav_uri_decode (char* source, char* dest )
{
  int nLength;
  for (nLength = 0; *source; nLength++) {
    dest[nLength+1] = 0;
    if (*source == '%' && source[1] && source[2] && isxdigit(source[1]) && isxdigit(source[2])) {
      source[1] -= source[1] <= '9' ? '0' : (source[1] <= 'F' ? 'A' : 'a')-10;
      source[2] -= source[2] <= '9' ? '0' : (source[2] <= 'F' ? 'A' : 'a')-10;
      dest[nLength] = 16 * source[1] + source[2];
      source += 3;
      continue;
    }
    dest[nLength] = *source++;
  }
  dest[nLength] = '\0';
  return nLength;
}


/*----------------------------------------------------------------------------*/
std::string
PropFindResponse::EncodeURI (const char* uri)
{

  XrdOucString nUri;
  char enc[ (strlen(uri) + 1) * 3];
  dav_uri_encode((unsigned char*) uri, enc, dav_rfc3986);
  return std::string(enc);
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
PropFindResponse::BuildResponse (eos::common::HttpRequest *request)
{
  using namespace rapidxml;

  // Get the namespaces (if any)
  ParseNamespaces();

  eos_static_debug("\n%s", request->GetBody().c_str());
  // Root node <propfind/>
  xml_node<> *rootNode = mXMLRequestDocument.first_node();
  if (!rootNode)
  {
    SetResponseCode(ResponseCodes::BAD_REQUEST);
    return this;
  }

  // Get the requested property types
  ParseRequestPropertyTypes(rootNode);
  
  if (mRequestPropertyTypes & PropertyTypes::GET_OCID) 
  {
    XrdOucErrInfo error;
    XrdOucString val;
    eos::common::Mapping::VirtualIdentity rootvid;
    eos::common::Mapping::Root(rootvid);
    if (gOFS->_attr_get(request->GetUrl().c_str(), error, rootvid, "", eos::common::OwnCloud::GetAllowSyncName(), val))
    {
      // Sync not allowed in this tree.
      SetResponseCode(ResponseCodes::METHOD_NOT_ALLOWED);
      return this;
    }
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

  multistatusNode->append_attribute(
                                    AllocateAttribute(eos::common::OwnCloud::OwnCloudNs(),
                                                      eos::common::OwnCloud::OwnCloudNsUrl()));

  mXMLResponseDocument.append_node(multistatusNode);

  // Is the requested resource a file or directory?
  XrdOucErrInfo error;
  struct stat statInfo;
  std::string etag;
  memset(&statInfo, 0, sizeof (struct stat));

  gOFS->_stat(request->GetUrl().c_str(), &statInfo, error, *mVirtualIdentity,
              (const char*) 0, &etag);

  // Figure out what we actually need to do
  std::string depth = request->GetHeaders()["depth"];

  // -----------------------------------------------------------------------------
  // Owncloud patch
  // -----------------------------------------------------------------------------

  //  if ( (depth == "1") && (request->GetHeaders()["user-agent"].find("csyncoC") != std::string::npos) )
  //  {
  //    depth = "1,noroot";
  //  }

  eos_static_debug("depth=%s, isdir=%d", depth.c_str(), S_ISDIR(statInfo.st_mode));
  xml_node<> *responseNode = 0;
  if (depth == "0" || !S_ISDIR(statInfo.st_mode))
  {
    // Simply stat the file or direcAtory
    responseNode = BuildResponseNode(request->GetUrl(), request->GetUrl(true));
    if (responseNode)
    {
      multistatusNode->append_node(responseNode);
    }
    else
    {
      return this;
    }
  }

  else if (depth == "1")
  {
    // Stat the resource and all child resources
    XrdMgmOfsDirectory directory;
    int listrc = directory._open(request->GetUrl().c_str(), *mVirtualIdentity,
                                (const char*) 0);

    responseNode = BuildResponseNode(request->GetUrl().c_str(), request->GetUrl(true).c_str());

    if (responseNode)
    {
      multistatusNode->append_node(responseNode);
    }

    if (!listrc)
    {
      const char *val;
      while ((val = directory.nextEntry()))
      {
        XrdOucString entryname = val;
	// don't display . .., atomic(+version) uploads and version directories
        if ( entryname.beginswith(EOS_COMMON_PATH_VERSION_FILE_PREFIX) ||
	     entryname.beginswith(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) ||
	     entryname.beginswith(EOS_WEBDAV_HIDE_IN_PROPFIND_PREFIX) || 
	     (entryname == ".") || 
	     (entryname == "..") )
	  {
          // skip over . .. and hidden files
          continue;
        }

        // one response node for each file...
        eos::common::Path path((request->GetUrl() + std::string("/") + std::string(val)).c_str());
        eos::common::Path refpath((request->GetUrl(true) + std::string("/") + std::string(val)).c_str());
        responseNode = BuildResponseNode(path.GetPath(), refpath.GetPath());
        if (responseNode)
        {
          multistatusNode->append_node(responseNode);
        }
        else
        {
          return this;
        }
      }
    }
    else
    {
      eos_static_warning("msg=\"error opening directory\"");
      SetResponseCode(HttpResponse::BAD_REQUEST);
      return this;
    }
  }

  else if (depth == "1,noroot")
  {
    // Stat all child resources but not the requested resource
    SetResponseCode(HttpResponse::NOT_IMPLEMENTED);
    return this;
  }

  else if (depth == "infinity" || depth == "")
  {
    // Recursively stat the resource and all child resources
    SetResponseCode(HttpResponse::NOT_IMPLEMENTED);
    return this;
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
void
PropFindResponse::ParseRequestPropertyTypes (rapidxml::xml_node<> *node)
{
  using namespace rapidxml;

  // <prop/> node (could be multiple, could be <allprop/>)
  xml_node<> *allpropNode = GetNode(node, "allprop");
  if (allpropNode)
  {
    mRequestPropertyTypes |= PropertyTypes::GET_CONTENT_LENGTH;
    mRequestPropertyTypes |= PropertyTypes::GET_CONTENT_TYPE;
    mRequestPropertyTypes |= PropertyTypes::GET_LAST_MODIFIED;
    mRequestPropertyTypes |= PropertyTypes::GET_ETAG;
    mRequestPropertyTypes |= PropertyTypes::CREATION_DATE;
    mRequestPropertyTypes |= PropertyTypes::DISPLAY_NAME;
    mRequestPropertyTypes |= PropertyTypes::RESOURCE_TYPE;
    mRequestPropertyTypes |= PropertyTypes::CHECKED_IN;
    mRequestPropertyTypes |= PropertyTypes::CHECKED_OUT;
    return;
  }

  // It wasn't <allprop/>
  xml_node<> *propNode = GetNode(node, "prop");
  if (!propNode)
  {
    eos_static_err("msg=\"no <prop/> node found in tree\"");
    return;
  }

  xml_node<> *property = propNode->first_node();

  // Find all the request properties
  while (property)
  {
    XrdOucString propertyName = property->name();
    eos_static_debug("msg=\"found xml property: %s\"", propertyName.c_str());

    int colon = 0;
    if ((colon = propertyName.find(':')) != STR_NPOS)
    {
      // Split node name into <ns>:<nodename>
      // Ignore non DAV: namespaces for now
      for (auto it = mDAVNamespaces.begin(); it != mDAVNamespaces.end(); ++it)
      {
        std::string ns = it->first;
        if (propertyName.beginswith(ns.c_str()))
        {
          std::string prop(std::string(propertyName.c_str()), colon + 1);
          mRequestPropertyTypes |= MapRequestPropertyType(prop);
        }
      }
    }
    else
    {
      std::string prop(propertyName.c_str());
      mRequestPropertyTypes |= MapRequestPropertyType(prop);
    }

    property = property->next_sibling();
  }
}

/*----------------------------------------------------------------------------*/
rapidxml::xml_node<>*
PropFindResponse::BuildResponseNode (const std::string &url, const std::string &hrefurl)
{
  using namespace rapidxml;

  XrdMgmOfsDirectory directory;
  XrdOucErrInfo error;
  struct stat statInfo;
  std::string etag;
  std::string id;

  XrdOucString urlp = url.c_str();
  XrdOucString hrefp = hrefurl.c_str();

  while (urlp.replace("//", "/"))
  {
  }
  while (hrefp.replace("//", "/"))
  {
  }

  // Is the requested resource a file or directory?
  eos_static_debug("url=%s", urlp.c_str());
  if (gOFS->_stat(urlp.c_str(), &statInfo, error, *mVirtualIdentity,
                  (const char*) 0, &etag))
  {
    eos_static_err("msg=\"error stating %s: %s\"", urlp.c_str(),
                   error.getErrText());
    SetResponseCode(ResponseCodes::NOT_FOUND);
    return NULL;
  }
  eos_static_debug("url=%s etag=%s", urlp.c_str(), etag.c_str());

  // encode the url's
  urlp = EncodeURI(urlp.c_str()).c_str();
  hrefp = EncodeURI(hrefp.c_str()).c_str();

  // <response/> node
  xml_node<> *responseNode = AllocateNode("d:response");

  // <href/> node
  xml_node<> *href = AllocateNode("d:href");

  if (S_ISDIR(statInfo.st_mode))
  {
    if (hrefp[hrefp.length() - 1] != '/')
      hrefp += "/";
  }

  SetValue(href, hrefp.c_str());
  responseNode->append_node(href);

  // <propstat/> node for "found" properties
  xml_node<> *propstatFound = AllocateNode("d:propstat");
  responseNode->append_node(propstatFound);

  // <status/> "found" node
  xml_node<> *statusFound = AllocateNode("d:status");
  SetValue(statusFound, "HTTP/1.1 200 OK");
  propstatFound->append_node(statusFound);

  // <prop/> "found" node
  xml_node<> *propFound = AllocateNode("d:prop");
  propstatFound->append_node(propFound);

  // <propstat/> node for "not found" properties
  xml_node<> *propstatNotFound = AllocateNode("d:propstat");
  responseNode->append_node(propstatNotFound);

  // <status/> "not found" node
  xml_node<> *statusNotFound = AllocateNode("d:status");
  SetValue(statusNotFound, "HTTP/1.1 404 Not Found");
  propstatNotFound->append_node(statusNotFound);

  // <prop/> "not found" node
  xml_node<> *propNotFound = AllocateNode("d:prop");
  propstatNotFound->append_node(propNotFound);

  xml_node<> *contentLength = 0;
  xml_node<> *lastModified = 0;
  xml_node<> *resourceType = 0;
  xml_node<> *checkedIn = 0;
  xml_node<> *checkedOut = 0;
  xml_node<> *creationDate = 0;
  xml_node<> *eTag = 0;
  xml_node<> *displayName = 0;
  xml_node<> *contentType = 0;
  xml_node<> *quotaAvail = 0;
  xml_node<> *quotaUsed = 0;
  xml_node<> *ocid = 0;

  if (mRequestPropertyTypes & PropertyTypes::GET_CONTENT_LENGTH)
    contentLength = AllocateNode("d:getcontentlength");
  if (mRequestPropertyTypes & PropertyTypes::GET_CONTENT_TYPE)
    contentType = AllocateNode("d:getcontenttype");
  if (mRequestPropertyTypes & PropertyTypes::GET_LAST_MODIFIED)
    lastModified = AllocateNode("d:getlastmodified");
  if (mRequestPropertyTypes & PropertyTypes::CREATION_DATE)
    creationDate = AllocateNode("d:creationdate");
  if (mRequestPropertyTypes & PropertyTypes::RESOURCE_TYPE)
    resourceType = AllocateNode("d:resourcetype");
  if (mRequestPropertyTypes & PropertyTypes::DISPLAY_NAME)
    displayName = AllocateNode("d:displayname");
  if (mRequestPropertyTypes & PropertyTypes::GET_ETAG)
    eTag = AllocateNode("d:getetag");
  if (mRequestPropertyTypes & PropertyTypes::CHECKED_IN)
    checkedIn = AllocateNode("d:checked-in");
  if (mRequestPropertyTypes & PropertyTypes::CHECKED_OUT)
    checkedOut = AllocateNode("d:checked-out");
  if (mRequestPropertyTypes & PropertyTypes::GET_OCID)
    ocid = AllocateNode("oc:id");

  if ((S_ISDIR(statInfo.st_mode)) &&
      ((mRequestPropertyTypes & PropertyTypes::QUOTA_AVAIL) ||
       (mRequestPropertyTypes & PropertyTypes::QUOTA_USED)))
  {
    // -----------------------------------------------------------
    // retrieve the current quota
    // -----------------------------------------------------------
    std::string path = urlp.c_str();
    if (path.substr(path.length() - 1, 1) != "/")
    {
      path += "/";
    }

    long long maxbytes = 0;
    long long freebytes = 0;
    Quota::GetIndividualQuota(*mVirtualIdentity, path.c_str(), maxbytes, freebytes);

    if (mRequestPropertyTypes & PropertyTypes::QUOTA_AVAIL)
    {
      std::string sQuotaAvail;
      quotaAvail = AllocateNode("d:quota-available-bytes");
      if (quotaAvail)
        SetValue(quotaAvail, eos::common::StringConversion::GetSizeString(sQuotaAvail, (unsigned long long) freebytes));
    }
    if (mRequestPropertyTypes & PropertyTypes::QUOTA_USED)
    {
      std::string sQuotaUsed;
      quotaUsed = AllocateNode("d:quota-used-bytes");
      if (quotaUsed)
        SetValue(quotaUsed, eos::common::StringConversion::GetSizeString(sQuotaUsed, (unsigned long long) maxbytes - freebytes));
    }
  }

  // getlastmodified, creationdate, displayname and getetag properties are
  // common to all resources
  if (lastModified)
  {
    std::string lm = eos::common::Timing::utctime(
                                                  statInfo.st_mtim.tv_sec);
    SetValue(lastModified, lm.c_str());
    propFound->append_node(lastModified);
  }

  if (creationDate)
  {
    std::string cd = eos::common::Timing::UnixTimstamp_to_ISO8601(
                                                                  statInfo.st_ctim.tv_sec);
    SetValue(creationDate, cd.c_str());
    propFound->append_node(creationDate);
  }

  if (eTag)
  {
    SetValue(eTag, etag.c_str());
    propFound->append_node(eTag);
  }

  if (ocid)
  {
    SetValue(ocid, eos::common::StringConversion::GetSizeString(id, (unsigned long long) statInfo.st_ino));
    propFound->append_node(ocid);
  }

  if (displayName)
  {
    eos::common::Path path(urlp.c_str());
    eos_static_debug("msg=\"display name: %s\"", path.GetName());
    SetValue(displayName, path.GetName());
    propFound->append_node(displayName);
  }

  // Directory
  if (S_ISDIR(statInfo.st_mode))
  {
    if (resourceType)
    {
      xml_node<> *container = AllocateNode("d:collection");
      resourceType->append_node(container);
      propFound->append_node(resourceType);
    }
    if (contentLength) propNotFound->append_node(contentLength);
    if (contentType)
    {
      SetValue(contentType, "httpd/unix-directory");
      propFound->append_node(contentType);
    }
    if (quotaAvail)
    {
      propFound->append_node(quotaAvail);
    }
    if (quotaUsed)
    {
      propFound->append_node(quotaUsed);
    }
  }

    // File
  else
  {
    if (resourceType) propNotFound->append_node(resourceType);
    if (contentLength)
    {
      SetValue(contentLength, std::to_string((long long) statInfo.st_size).c_str());
      propFound->append_node(contentLength);
    }
    if (contentType)
    {
      SetValue(contentType, HttpResponse::ContentType(url.c_str()).c_str());
      propFound->append_node(contentType);
    }
  }

  // We don't use these (yet)
  if (checkedIn) propNotFound->append_node(checkedIn);
  if (checkedOut) propNotFound->append_node(checkedOut);

  return responseNode;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
