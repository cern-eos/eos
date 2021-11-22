//------------------------------------------------------------------------------
// File: OAuth.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "common/Namespace.hh"
#include "common/OAuth.hh"
#include "common/StringConversion.hh"
#include "jwt-cpp/jwt.h"
#include <iostream>
#include <memory>
#include <curl/curl.h>
#include <curl/easy.h>
#include <json/json.h>


#include "common/Murmur3.hh"

EOSCOMMONNAMESPACE_BEGIN;

void
OAuth::Init()
{
  return;
}


std::size_t
OAuth::callback(const char* in, std::size_t size,
                std::size_t num, std::string* out)
{
  const std::size_t totalBytes(size * num);
  out->append(in, totalBytes);
  return totalBytes;
}



void
OAuth::PurgeCache(time_t& now)
{
  static time_t last_purge = time(NULL);
  eos::common::RWMutexWriteLock lock(mOAuthCacheMutex);

  // purge every 5 min if we have more than 64k entries or every hour for less
  if (((mOAuthInfo.size() > 65336) && (now - last_purge) > 300) ||
      ((now - last_purge) > 3600)) {
    for (auto it = mOAuthInfo.begin(); it != mOAuthInfo.end();) {
      time_t ctime = strtoull(it->second["ctime"].c_str(), 0, 10);
      time_t etime = strtoull(it->second["etime"].c_str(), 0, 10);

      if (
        (etime && (etime < now)) ||
        ((now - ctime) > cache_validity_time)) {
        it = mOAuthInfo.erase(it);
      } else {
        ++it;
      }
    }
  }
}

int
OAuth::Validate(OAuth::AuthInfo& info, const std::string& accesstoken,
                const std::string& resource, const std::string& refreshtoken, time_t& expires)
{
  time_t now = time(NULL);

  if (expires && (expires < now)) {
    return ETIME;
  }

  // screen the audience
  auto decoded = jwt::decode(accesstoken);
  auto audiences = decoded.get_audience();
  auto exp = decoded.get_expires_at();
  expires = std::chrono::system_clock::to_time_t(exp);
  bool audience_match = false;
  std::stringstream s;

  for (auto& e : decoded.get_payload_claims()) {
    s << e.first << "=" << e.second << " ";
  }

  eos_static_info("token='%s...' claims=[ %s ]",
                  accesstoken.substr(0, 20).c_str(),
                  s.str().c_str());

  if (Mapping::IsOAuth2Resource(resource)) {
    // no audience require
    audience_match = true;
  } else {
    for (auto it = audiences.begin(); it != audiences.end(); ++it) {
      std::string audience_resource = resource + "@";
      audience_resource += *it;

      if (Mapping::IsOAuth2Resource(audience_resource)) {
        audience_match = true;
        break;
      }
    }
  }

  if (!audience_match) {
    eos_static_err("msg=\"rejecing - no audience matches\"");
    return EPERM;
  }

  // get the hash
  uint64_t tokenhash = Hash(accesstoken);
  PurgeCache(now);
  {
    eos::common::RWMutexReadLock lock(mOAuthCacheMutex);
    auto cache = mOAuthInfo.find(tokenhash);

    if (cache != mOAuthInfo.end()) {
      time_t ctime = strtoull(cache->second["ctime"].c_str(), 0, 10);
      time_t etime = strtoull(cache->second["etime"].c_str(), 0, 10);

      if ((!etime) || (etime > now)) {
        if ((now - ctime) < cache_validity_time) {
          info = cache->second;
          return 0;
        }
      }
    }
  }
  auto curl = curl_easy_init();

  if (curl) {
    std::string httpsresource = std::string("https://") + resource;
    curl_easy_setopt(curl, CURLOPT_URL, httpsresource.c_str());
    long httpCode(0);
    std::unique_ptr<std::string> httpData(new std::string());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, httpData.get());
    std::string auth = "Authorization: Bearer ";
    auth += accesstoken;
    auto chunk = curl_slist_append(NULL, auth.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

    if (EOS_LOGS_DEBUG) {
      curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
    }

    curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    curl_easy_cleanup(curl);

    if (httpCode == 200) {
      Json::Value jsonData;
      Json::Reader jsonReader;

      if (jsonReader.parse(*httpData.get(), jsonData)) {
        if (EOS_LOGS_DEBUG) {
          std::cerr << "Successfully parsed JSON data" << std::endl;
          std::cerr << "\nJSON data received:" << std::endl;
          std::cerr << jsonData.toStyledString() << std::endl;
        }

        if (jsonData.isMember("name")) {
          info["name"] = jsonData["name"].asString();
        }

        if (jsonData.isMember("username")) {
          // OAuth style
          info["username"] = jsonData["username"].asString();
        } else {
	  if (getenv("EOS_MGM_OIDC_MAP_FIELD") && jsonData.isMember(getenv("EOS_MGM_OIDC_MAP_FIELD"))) {
	    // configuration overwrite of field to map
	    info["username"] = jsonData[getenv("EOS_MGM_OIDC_MAP_FIELD")].asString();
	  } else {
	    // OIDC style
	    if (jsonData.isMember("sub")) {
	      info["username"] = jsonData["sub"].asString();
	    } else {
	      // we need to have this field to map someone
	      return EINVAL;
	    }
	  }
        }

        if (jsonData.isMember("email")) {
          info["email"] = jsonData["email"].asString();
        }

        if (jsonData.isMember("federation")) {
          info["federation"] = jsonData["federation"].asString();
        }

        // cache this entry
        info["ctime"] = std::to_string(time(NULL));
        info["etime"] = expires ? std::to_string(expires) : std::to_string(
                          now + cache_validity_time);
        eos::common::RWMutexWriteLock lock(mOAuthCacheMutex);
        mOAuthInfo[tokenhash] = info;
        return 0;
      } else {
        return EINVAL;
      }
    } else {
      return (int)httpCode;
    }
  }

  return EFAULT;
}

std::string
OAuth::Handle(const std::string& info, eos::common::VirtualIdentity& vid)
{
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(info, tokens, ":");

  if (tokens.size() > 1) {
    if (tokens[0] == "oauth2") {
      if (tokens.size() < 3) {
        tokens.push_back("");
      }

      if (tokens.size() < 4) {
        tokens.push_back("0");
      }

      if (tokens.size() < 5) {
        tokens.push_back("");
      }

      OAuth::AuthInfo oinfo;
      time_t expires = strtoull(tokens[3].c_str(), 0, 10);

      if (!Validate(oinfo, tokens[1], tokens[2], tokens[4], expires)) {
        // valid token, now map the user name
        eos_static_info("username='%s' name='%s' federation='%s' email='%s' expires=%llu",
                        oinfo["username"].c_str(),
                        oinfo["name"].c_str(),
                        oinfo["federation"].c_str(),
                        oinfo["email"].c_str(),
                        expires);
        vid.federation = oinfo["federation"];
        vid.email = oinfo["email"];
        vid.fullname = oinfo["name"];
        return oinfo["username"];
      }
    }
  }

  return "";
}

uint64_t
OAuth::Hash(const std::string& token)
{
  //  std::cerr << "hashing token: " << token << std::endl;
  //  std::cerr << "hash: " <<  Murmur3::MurmurHasher<std::string> {}(token) << std::endl;
  return Murmur3::MurmurHasher<std::string> {}(token);
}

EOSCOMMONNAMESPACE_END;
