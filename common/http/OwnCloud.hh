// ----------------------------------------------------------------------
// File: OwnCloud.hh
// Author: Andreas-Joachim Peters CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
 * @file   OwnCloud.hh
 *
 * @brief  Deals with OwnCloud specific headers and naming conventions
 */

#ifndef __EOSCOMMON_OWNCLOUD__HH__
#define __EOSCOMMON_OWNCLOUD__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/http/HttpRequest.hh"
#include "common/http/HttpResponse.hh"
#include "common/http/HttpServer.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
#include <map>
#include <string>

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class OwnCloudPath : public Path {
public:
  int mMaxChunks; //< max OC index for a chunked path
  int mNChunk; //< OC index for a chunked path
  XrdOucString mUploadId; //< OC client id for a chunked path

  // ---------------------------------------------------------------------------
  //! Parse a chunked path into pieces
  //! OC used <basename>-<id>-<max-chunks>-<n-chunk>
  // ---------------------------------------------------------------------------

  OwnCloudPath (const char* p) : Path (p)
  {
  }

  const char* ParseChunkedPath ()
  {
    atomicPath = GetFullPath();
    int lOCnChunk, lOCmaxChunks;
    lOCnChunk = lOCmaxChunks = 0;
    XrdOucString lOCuploadId;
    XrdOucString unchunkedPath = atomicPath;

    int pos;

    if ((pos = unchunkedPath.rfind("-")) != STR_NPOS)
    {
      lOCnChunk = atoi(unchunkedPath.c_str() + pos + 1);
      atomicPath.erase(pos);
      unchunkedPath.erase(pos);

      if ((pos = unchunkedPath.rfind("-")) != STR_NPOS)
      {
        unchunkedPath.erase(pos);
        lOCmaxChunks = atoi(unchunkedPath.c_str() + pos + 1);
        if ((pos = unchunkedPath.rfind("-", pos - 1)) != STR_NPOS)
        {
          lOCuploadId = unchunkedPath.c_str() + pos + 1;
          unchunkedPath.erase(pos);
          if ((pos = unchunkedPath.rfind("-", pos - 1)) != STR_NPOS)
          {
            if (unchunkedPath.endswith("-chunking"))
            {
              // remove -chunking at the end
              unchunkedPath.erase(pos);
            }
          }
        }
      }

    }
    Init(unchunkedPath.c_str());
    mNChunk = lOCnChunk;
    mMaxChunks = lOCmaxChunks;
    mUploadId = lOCuploadId;
    return GetPath();
  }
};

/*----------------------------------------------------------------------------*/
class OwnCloud {
private:
public:

  // ---------------------------------------------------------------------------

  static bool isChunkUpload (HttpRequest* request)
  {
    return request->GetHeaders().count("oc-chunked");
  }

  static bool isChunkUpload (XrdOucEnv& env)
  {
    return ( env.Get("oc-chunk-n") ? true : false);
  }

  // ---------------------------------------------------------------------------

  static const char* getContentSize (HttpRequest* request)
  {
    if (request->GetHeaders().count("oc-total-length"))
      return request->GetHeaders()["oc-total-length"].c_str();
    else
      return 0;
  }

  // ---------------------------------------------------------------------------

  static bool GetChunkInfo (const char* request,
                            int &chunk_n,
                            int &chunk_max,
                            XrdOucString &chunk_uuid)
  {
    bool ok = true;
    XrdOucEnv env(request);
    const char* val = 0;
    if ((val = env.Get("oc-chunk-n")))
      chunk_n = (int) strtol(val, 0, 10);
    else
      ok = false;
    if ((val = env.Get("oc-chunk-max")))
      chunk_max = (int) strtol(val, 0, 10);
    else
      ok = false;
    if ((val = env.Get("oc-chunk-uuid")))
      chunk_uuid = val;
    else
      ok = false;
    return ok;
  }

  // ---------------------------------------------------------------------------

  static int GetNChunk (HttpRequest* request)
  {
    if (!request->GetHeaders().count("oc-chunk-n"))
      return 0;

    else
      return (int) strtol(request->GetHeaders()["oc-chunk-n"].c_str(), 0, 10);
  }

  // ---------------------------------------------------------------------------

  typedef std::pair<std::string, std::string> checksum_t;

  static checksum_t GetChecksum (HttpRequest* request)
  {
    if (!request->GetHeaders().count("oc-checksum"))
      return std::make_pair<std::string, std::string>("", "");

    std::string checksum_data = request->GetHeaders()["oc-checksum"];
    std::string checksum_type = checksum_data;
    std::string checksum_value = checksum_data;
    size_t epos = checksum_data.find(":");
    if (epos != std::string::npos) {
      checksum_value.erase(0, checksum_data.find(":") + 1);
      checksum_type.erase(checksum_data.find(":"));
    }

    checksum_type = LC_STRING(checksum_type);

    // map checksum types to EOS checksum names
    if (checksum_type == "adler32")
      checksum_type = "adler";

    return std::make_pair (checksum_type, checksum_value);
  }

  // ---------------------------------------------------------------------------

  static std::string GetChecksumString (std::string type, std::string value)
  {
    std::string checksum;
    if (type == "adler")
      checksum += "Adler32";
    else if (type == "md5")
      checksum += "MD5";
    else if (type == "sha1")
      checksum += "SHA1";
    else if (type == "crc32c")
      checksum += "CRC32C";
    else if (type == "crc32")
      checksum += "CRC32";
    else
      checksum += "unknown";
    checksum += ":";
    checksum += value;
    return checksum;
  }

  // ---------------------------------------------------------------------------

  static int GetMaxChunks (HttpRequest* request)
  {
    if (!request->GetHeaders().count("oc-chunk-max"))
      return 0;

    else
      return (int) strtol(request->GetHeaders()["oc-chunk-max"].c_str(), 0, 10);
  }

  // ---------------------------------------------------------------------------

  static bool HasOcContentLength (HttpRequest* request)
  {

    return request->GetHeaders().count("oc-total-length");
  }

  // ---------------------------------------------------------------------------

  static void ReplaceRemotePhp (XrdOucString& path)
  {
    if (path.find("/remote.php/webdav/") != STR_NPOS)
    {

      path.replace("remote.php/webdav/", "");
    }
  }
  // ---------------------------------------------------------------------------

  static bool WantsStatus (XrdOucString& path)
  {
    if (path.find("/status.php") != STR_NPOS)
    {
      path.replace("/status.php", "");
      return true;
    }
    else
    {

      return false;
    }
  }

  // ---------------------------------------------------------------------------

  static std::string prepareChunkUpload (HttpRequest* request,
                                         HttpResponse** response,
                                         std::map<std::string, std::string> &ocHeader)
  {
    eos::common::OwnCloudPath ocPath(request->GetUrl().c_str());
    ocPath.ParseChunkedPath();

    eos_static_info("type=\"oc-chunked\" in-path=\"%s\" final-path=\"%s\""
                    " id=\"%s\" n=%d max=%d",
                    request->GetUrl().c_str(),
                    ocPath.GetFullPath().c_str(),
                    ocPath.mUploadId.c_str(),
                    ocPath.mNChunk,
                    ocPath.mMaxChunks
                    );

    if (ocPath.mMaxChunks > 0xffff)
    {
      // -----------------------------------------------------------------------
      // we support maximum 65536 chunks
      // the reason is, that we can only store 16-bit under the flags entry
      // in the namespace meta data per file
      // -----------------------------------------------------------------------
      *response = HttpServer::HttpError("Too many chunks to upload (>65536)",
                                        EOPNOTSUPP);
      return "";
    }

    XrdOucString OcMaxChunks = "";
    OcMaxChunks += (int) ocPath.mMaxChunks;
    XrdOucString OcNChunk = "";
    OcNChunk += (int) ocPath.mNChunk;
    XrdOucString OcUuid = "";
    OcUuid += ocPath.mUploadId.c_str();
    int pad = 36 - ocPath.mUploadId.length();
    if (pad > 0)
    {
      for (int i = 0; i < pad; i++)
        OcUuid += "0";
    }
    if (pad < 0)
    {

      OcUuid.erase(OcUuid.length() + pad);
    }

    // -------------------------------------------------------------------------
    // return some
    // -------------------------------------------------------------------------
    ocHeader["oc-chunk-n"] = OcNChunk.c_str();
    ocHeader["oc-chunk-max"] = OcMaxChunks.c_str();
    ocHeader["oc-chunk-uuid"] = OcUuid.c_str();

    // -------------------------------------------------------------------------
    // we return the final path
    // -------------------------------------------------------------------------
    return ocPath.GetFullPath().c_str();
  }

  // ---------------------------------------------------------------------------

  static void addOcHeader (HttpResponse* response,
                           std::map<std::string, std::string> &ocHeader)
  {
    for (auto it = ocHeader.begin(); it != ocHeader.end(); ++it)
    {

      response->AddHeader(it->first, it->second);
    }
  }

  // ---------------------------------------------------------------------------

  static XrdOucString HeaderToQuery (std::map<std::string, std::string> &ocHeader)
  {
    XrdOucString query;
    for (auto it = ocHeader.begin(); it != ocHeader.end(); ++it)
    {
      if (it->first.substr(0, 3) == "oc-")
      {

        query += "&";
        query += it->first.c_str();
        query += "=";
        query += it->second.c_str();
      }
    }
    return query;
  }

  // ---------------------------------------------------------------------------

  static XrdOucString FilterOcQuery (const char* query)
  {
    XrdOucString filterQuery;
    XrdOucEnv queryEnv(query);
    int envlen;
    std::map<std::string, std::string> map;
    eos::common::StringConversion::GetKeyValueMap(
                                                  queryEnv.Env(envlen), map, "=", "&");

    for (auto it = map.begin(); it != map.end(); ++it)
    {
      if (it->first.substr(0, 3) == "oc-")
      {

        filterQuery += "&";
        filterQuery += it->first.c_str();
        filterQuery += "=";
        filterQuery += it->second.c_str();
      }
    }
    return filterQuery;

  }

  // ---------------------------------------------------------------------------

  static const char* OwnCloudNs ()
  {

    return "xmlns:oc";
  }

  // ---------------------------------------------------------------------------

  static const char* OwnCloudNsUrl ()
  {

    return "http://owncloud.org/ns";
  }

  // ---------------------------------------------------------------------------

  static const char* OwnCloudRemapping (XrdOucString& path, HttpRequest* request)
  {
    XrdOucString client_path = "";
    XrdOucString server_path = "";
    if (request->GetHeaders().count("cbox-client-mapping"))
    {
      client_path = request->GetHeaders()["cbox-client-mapping"].c_str();
    }

    if (request->GetHeaders().count("cbox-server-mapping"))
    {
      server_path = request->GetHeaders()["cbox-server-mapping"].c_str();
    }

    while (path.replace("//", "/"))
    {
    }

    if (!path.beginswith("/"))
      path.insert("/", 0);

    // shortcut if there is nothing to replace
    if (!client_path.length())
      return path.c_str();

    while (client_path.replace("//", "/"))
    {
    }
    while (server_path.replace("//", "/"))
    {
    }
    if (!client_path.beginswith("/"))
      client_path.insert("/", 0);

    if (!server_path.beginswith("/"))
      server_path.insert("/", 0);

    path.replace(client_path, server_path);
    return path.c_str();
  }

  // ---------------------------------------------------------------------------

  static const char* GetAllowSyncName ()
  {
    return "sys.allow.oc.sync";
  }

};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
