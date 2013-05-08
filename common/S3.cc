// ----------------------------------------------------------------------
// File: S3.cc
// Author: Andreas-Joachim Peters - CERN
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

/*----------------------------------------------------------------------------*/
#include "common/S3.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
S3::S3 ()
{
  //.............................................................................
  // Constructor
  //.............................................................................
  mIsS3 = false;
  mId = mSignature = mHost = mContentMD5 = mContentType = mUserAgent = "";
  mHttpMethod = mPath = mQuery = mBucket = mDate = "";
  mVirtualHost = false;

}

/*----------------------------------------------------------------------------*/
S3::~S3 () {
  //.............................................................................
  // Destructor
  //.............................................................................
}

/*----------------------------------------------------------------------------*/
void
S3::ParseHeader (std::map<std::string, std::string> &header)
{
  //.............................................................................
  // Parse function to analyse HTTP and AMZ header map
  //.............................................................................
  std::string header_line;
  for (auto it = header.begin(); it != header.end(); it++)
  {
    header_line += it->first;
    header_line += "=";
    header_line += it->second;
    header_line += " ";
  }
  eos_static_info("%s", header_line.c_str());
  if (header.count("Authorization"))
  {
    if (header["Authorization"].substr(0, 3) == "AWS")
    {
      // this is amanzon webservice authorization
      mId = header["Authorization"].substr(4);
      mSignature = mId;
      size_t dpos = mId.find(":");
      if (dpos != std::string::npos)
      {
        mId.erase(dpos);
        mSignature.erase(0, dpos + 1);

        if (header.count("HttpMethod"))
        {
          mHttpMethod = header["HttpMethod"];
        }
        if (header.count("Path"))
        {
          mPath = header["Path"];
          std::string subdomain = SubDomain(header["Host"]);

          if (subdomain.length())
          {
            // implementation for DNS buckets
            mBucket = subdomain;
	    mVirtualHost = true;
          }
          else
          {
	    mVirtualHost = false;
            // implementation for non DNS buckets
            mBucket = mPath;

            if (mBucket[0] == '/')
            {
              mBucket.erase(0, 1);
            }

            size_t slash_pos = mBucket.find("/");
            if (slash_pos != std::string::npos)
            {
              // something like data/...    

              mPath = mBucket;
              mPath.erase(0, slash_pos);
              mBucket.erase(slash_pos);
            }
            else
            {
              mPath = "/";
            }
          }
        }

        if (header.count("Query"))
        {
          mQuery = header["Query"];
        }
        if (header.count("Content-MD5"))
        {
          mContentMD5 = header["Content-MD5"];
        }
        if (header.count("Date"))
        {
          mDate = header["Date"];
        }
        if (header.count("content-type"))
        {
          mContentType = header["content-type"];
        }
        if (header.count("Host"))
        {
          mHost = header["Host"];
        }
        if (header.count("User-Agent"))
        {
          mUserAgent = header["User-Agent"];
        }
        // canonical amz header
        for (auto it = header.begin(); it != header.end(); it++)
        {
          XrdOucString amzstring = it->first.c_str();
          XrdOucString amzfield = it->second.c_str();
          // make lower case
          amzstring.lower(0);

          if (!amzstring.beginswith("x-amz-"))
          {
            // skip everything which is not amazon style
            continue;
          }
          // trim white space in the beginning
          while (amzfield.beginswith(" "))
          {
            amzfield.erase(0, 1);
          }
          int pos;
          // remove line folding and spaces after folding
          while ((pos = amzfield.find("\r\n ")) != STR_NPOS)
          {
            amzfield.erase(pos, 3);
            while (amzfield[pos] == ' ')
            {
              amzfield.erase(pos, 1);
            }
          }
          if (!mAmzMap.count(amzstring.c_str()))
          {
            mAmzMap[amzstring.c_str()] = amzfield.c_str();
          }
          else
          {
            mAmzMap[amzstring.c_str()] += ",";
            mAmzMap[amzstring.c_str()] += amzfield.c_str();
          }
        }
        // build a canonicalized resource
        for (auto it = mAmzMap.begin(); it != mAmzMap.end(); it++)
        {
          mCanonicalizedAmzHeaders += it->first;
          mCanonicalizedAmzHeaders += ":";
          mCanonicalizedAmzHeaders += it->second;
          mCanonicalizedAmzHeaders += "\n";
        }
        mIsS3 = true;
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
bool
S3::IsS3 ()
{
  //.............................................................................
  // Check if S3 object is complete
  //.............................................................................
  return mIsS3;
}

void
S3::Print (std::string & out)
{
  //.............................................................................
  // Print the S3 object contents to out
  //.............................................................................
  out = "id=";
  out += mId.c_str();
  out += " ";
  out += "signature=";
  out += mSignature.c_str();
  return;
}

std::string
S3::extractSubResource ()
{
  //.............................................................................
  // Extract everything from the query which is a sub-resource aka used for signatures
  //.............................................................................
  std::vector<std::string> srvec;
  eos::common::StringConversion::Tokenize(getQuery(), srvec, "&");
  for (auto it = srvec.begin(); it != srvec.end(); it++)
  {
    std::string key;
    std::string value;
    eos::common::StringConversion::SplitKeyValue(*it, key, value);
    if ((key == "acl") ||
        (key == "lifecycle") ||
        (key == "location") ||
        (key == "logging") ||
        (key == "delete") ||
        (key == "notification") ||
        (key == "uploads") ||
        (key == "partNumber") ||
        (key == "requestPayment") ||
        (key == "uploadId") ||
        (key == "versionId") ||
        (key == "versioning") ||
        (key == "versions") ||
        (key == "website") ||
        (key == "torrent"))
    {
      mSubResourceMap[key] = value;
    }
  }
  mSubResource = "";
  for (auto it = mSubResourceMap.begin(); it != mSubResourceMap.end(); it++)
  {
    if (mSubResource.length())
    {
      mSubResource += "&";
    }
    mSubResource += it->first;
    mSubResource += "=";
    mSubResource += it->second;
  }
  return mSubResource;
}

bool
S3::VerifySignature (std::string secure_key)
{
  std::string string2sign = getHttpMethod();
  string2sign += "\n";
  string2sign += getContentMD5();
  string2sign += "\n";
  string2sign += getContentType();
  string2sign += "\n";
  string2sign += getDate();
  string2sign += "\n";
  string2sign += getCanonicalizedAmzHeaders();


  if (getBucket().length())
  {
    string2sign += "/";
    string2sign += getBucket();
  };

  string2sign += getPath();

  if (extractSubResource().length())
  {
    string2sign += "?";
    string2sign += getSubResource();
  }

  eos_static_info("s2sign=%s key=%s", string2sign.c_str(), secure_key.c_str());

  // get hmac sha1 hash
  std::string hmac1 = eos::common::SymKey::HmacSha1(secure_key,
                                                    string2sign);

  XrdOucString b64mac1;
  // base64 encode the hash
  eos::common::SymKey::Base64Encode((char*) hmac1.c_str(), hmac1.size(), b64mac1);
  std::string verify_signature = b64mac1.c_str();
  eos_static_info("in_signature=%s out_signature=%s\n", getSignature().c_str(), verify_signature.c_str());
  return (verify_signature == getSignature());
}

/*----------------------------------------------------------------------------*/
S3 *
S3::ParseS3 (std::map<std::string, std::string> &header)
{
  S3* s3 = new S3();
  s3->ParseHeader(header);
  if (s3->IsS3())
  {
    return s3;
  }
  else
  {
    delete s3;
    return 0;
  }
}

/*----------------------------------------------------------------------------*/
std::string
S3::RestErrorResponse (int &response_code, int http_code, std::string errcode, std::string errmsg, std::string resource, std::string requestid)
{
  //.............................................................................
  // Creates a AWS RestError Response string
  //.............................................................................
  response_code = http_code;
  std::string result = XML_V1_UTF8;
  result += "<Error><Code>";
  result += errcode;
  result += "</Code>";
  result += "<Message>";
  result += errmsg;
  result += "</Message>";
  result += "<Resource>";
  result += resource;
  result += "</Resource>";
  result += "<RequestId>";
  result += requestid;
  result += "</RequestId>";
  result += "</Error";
  return result;
}

/*----------------------------------------------------------------------------*/
std::string
S3::ContentType ()
{
  XrdOucString name = getPath().c_str();
  if (name.endswith(".txt") ||
      name.endswith(".log"))
  {
    return "text/plain";
  }
  if (name.endswith(".xml"))
  {
    return "text/xml";
  }
  if (name.endswith(".gif"))
  {
    return "image/gif";
  }
  if (name.endswith(".jpg"))
  {
    return "image/jpg";
  }
  if (name.endswith(".png"))
  {
    return "image/png";
  }
  if (name.endswith(".tiff"))
  {
    return "image/tiff";
  }
  if (name.endswith(".mp3"))
  {
    return "audio/mp3";
  }
  if (name.endswith(".mp4"))
  {
    return "audio/mp4";
  }
  if (name.endswith(".pdf"))
  {
    return "application/pdf";
  }
  if (name.endswith(".zip"))
  {
    return "application/zip";
  }
  if (name.endswith(".gzip"))
  {
    return "application/gzip";
  }
  if (name.endswith(".tar.gz"))
  {
    return "application/gzip";
  }
  // default is binary stream
  return "application/octet-stream";
}

/*----------------------------------------------------------------------------*/
std::string
S3::SubDomain (std::string hostname)
{
  std::string subdomain = "";
  size_t pos1 = hostname.rfind(".");
  size_t pos2 = hostname.substr(0, pos1).rfind(".");
  size_t pos3 = hostname.substr(0, pos2).rfind(".");

  if ((pos1 != pos2) &&
      (pos2 != pos3) &&
      (pos1 != pos3) &&
      (pos1 != std::string::npos) &&
      (pos2 != std::string::npos) &&
      (pos3 != std::string::npos))
  {
    subdomain = hostname;
    subdomain.erase(pos3);
  }

  return subdomain;
}

EOSCOMMONNAMESPACE_END
