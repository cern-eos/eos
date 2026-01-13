// ----------------------------------------------------------------------
// File: Devices.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/****************************A********************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_DEVICES__HH__
#define __EOSMGM_DEVICES__HH__

#include "mgm/Namespace.hh"
#include "common/AssistedThread.hh"
#include "common/Timing.hh"
#include <XrdOuc/XrdOucString.hh>
#include <sys/types.h>

class XrdOucErrInfo;

EOSMGMNAMESPACE_BEGIN

/**
 * @file   Devices.hh
 *
 * @brief  This class implements the thread 
 * storing regulary device information to the proc filesystem
 */

class Devices
{
public:
  //----------------------------------------------------------------------------
  //! Default Constructor - use it to run the Devices thread by callign Start
  //! afterwards.
  //----------------------------------------------------------------------------

  Devices() { }

  ~Devices()
  {
    Stop();
  }

  void SetDevicesPath(const char* procpath) {
    mDevicesPath = procpath;
  }
  
  /* Start the devices thread 
   */
  bool Start();

  /* Stop the devices thread
   */
  void Stop();

  typedef std::map<uint64_t, std::string> json_map;
  typedef std::shared_ptr<json_map> json_map_t;
  typedef std::map<uint64_t, std::string> space_map;
  typedef std::shared_ptr<space_map> space_map_t;
  typedef std::map<uint64_t, std::string> smart_map;
  typedef std::shared_ptr<smart_map> smart_map_t;
  
  space_map_t getSpaceMap() {
    std::unique_lock<std::mutex> scope_lock(fsJsonMutex);
    return spaceMap;
  }
  
  json_map_t getJson() {
    std::unique_lock<std::mutex> scope_lock(fsJsonMutex);
    return fsJson;
  }

  smart_map_t getSmartMap() {
    std::unique_lock<std::mutex> scope_lock(fsJsonMutex);
    return smartMap;
  }
  
  std::string getLocalExtractionTime() const {
    time_t t = lastExtraction;
    return eos::common::Timing::ltime(t);
  }

  time_t getExtractionTime() const {
    return lastExtraction;
  }
  
  void Extract(); // extract from MQ messaging infos
  
private:
  AssistedThread mThread; ///< Thread doing the device extraction
  std::string mDevicesPath;
  std::mutex fsJsonMutex;
  json_map_t fsJson; // contains fsid->json info
  space_map_t spaceMap; // contains fsid->space info
  smart_map_t smartMap; // contains fsid->smart status
  std::atomic<time_t> lastExtraction;
  
  void Recorder(ThreadAssistant& assistant) noexcept;
  void Store(); // store into persistent namespace proc directory
  void setJson(json_map_t newjson) {
    std::unique_lock<std::mutex> scope_lock(fsJsonMutex);
    fsJson = newjson;
  }
  void setSpaceMap(space_map_t newspacemap) {
    std::unique_lock<std::mutex> scope_lock(fsJsonMutex);
    spaceMap = newspacemap;
  }
  void setSmartMap(space_map_t newsmartmap) {
    std::unique_lock<std::mutex> scope_lock(fsJsonMutex);
    smartMap = newsmartmap;
  }
};

EOSMGMNAMESPACE_END

#endif
