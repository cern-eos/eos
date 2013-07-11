// ----------------------------------------------------------------------
// File: S3Handler.cc
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
#include "common/http/s3/S3Handler.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
S3Handler::ParseHeader (eos::common::HttpRequest *request)
{
  HeaderMap header = request->GetHeaders();
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

        mHttpMethod = request->GetMethod();

        mPath = request->GetUrl();
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

        mQuery = request->GetQuery();

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
        if (header.count("Content-type"))
        {
          mContentType = header["Content-type"];
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
S3Handler::IsS3 ()
{
  // Check if S3 object is complete
  return mIsS3;
}

/*----------------------------------------------------------------------------*/
void
S3Handler::Print (std::string & out)
{
  // Print the S3 object contents to out
  out = "id=";
  out += mId.c_str();
  out += " ";
  out += "signature=";
  out += mSignature.c_str();
  return;
}

/*----------------------------------------------------------------------------*/
std::string
S3Handler::extractSubResource ()
{
  // Extract everything from the query which is a sub-resource aka used for
  // signatures
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

/*----------------------------------------------------------------------------*/
bool
S3Handler::VerifySignature (std::string secure_key)
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
  eos_static_info("in_signature=%s out_signature=%s\n",
                  getSignature().c_str(), verify_signature.c_str());
  return (verify_signature == getSignature());
}

/*----------------------------------------------------------------------------*/
std::string
S3Handler::SubDomain (std::string hostname)
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

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
