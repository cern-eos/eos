//------------------------------------------------------------------------------
//! @file cfskey.hh
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing the quota en-/disabling
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#pragma once

#include <string>
#include <curl/curl.h>
#include <curl/easy.h>

class cfskey
{
public:

  static std::size_t
  callback(const char* in, std::size_t size,
           std::size_t num, std::string* out)
  {
    const std::size_t totalBytes(size * num);
    out->append(in, totalBytes);
    return totalBytes;
  }


  static std::string get(std::string resource)
  {
    std::string httpsresource = std::string("https://") + resource;
    auto curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_URL, httpsresource.c_str());
    long httpCode(0);
    std::unique_ptr<std::string> httpData(new std::string());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, httpData.get());
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
    curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    curl_easy_cleanup(curl);

    if (httpCode == 200) {
      return *httpData;
    } else {
      return "";
    }
  }
private:
};
