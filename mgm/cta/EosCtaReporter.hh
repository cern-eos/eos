//------------------------------------------------------------------------------
// File: EosCtaReporter.hh
// Author: Joao Afonso - CERN
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

#include <vector>
#include <map>
#include <string>
#include <functional>

namespace cta {

// All EOS-CTA Report fields should be listed here
// ** NOTE: *** Parameters will be ordered as they are presented here
enum class ReportParam {
  // Basic params
  LOG,
  PATH,
  RUID,
  RGID,
  TD,
  HOST,
  TS,
  TNS,

  // Prepare request params
  PREP_REQ_EVENT,
  PREP_REQ_REQID,
  PREP_REQ_SENTTOWFE,
  PREP_REQ_SUCCESSFUL,
  PREP_REQ_ERROR,

  // WFE params
  PREP_WFE_EVENT,
  PREP_WFE_REQID,
  PREP_WFE_REQCOUNT,
  PREP_WFE_EVICTCOUNTER,
  PREP_WFE_ONDISK,
  PREP_WFE_ONTAPE,
  PREP_WFE_FIRSTPREPARE,
  PREP_WFE_SENTTOCTA,
  PREP_WFE_ACTIVITY,
  PREP_WFE_ERROR,

  // Evict cmd params
  EVICTCMD_EVICTCOUNTER,
  EVICTCMD_FILEREMOVED,
  EVICTCMD_ERROR,
  EVICTCMD_FSID,

  // File deletion params
  FILE_DEL_FID,
  FILE_DEL_FXID,
  FILE_DEL_EOS_BTIME,
  FILE_DEL_ARCHIVE_FILE_ID,
  FILE_DEL_ARCHIVE_STORAGE_CLASS,
  FILE_DEL_LOCATIONS,
  FILE_DEL_CHECKSUMTYPE,
  FILE_DEL_CHECKSUMVALUE,
  FILE_DEL_SIZE,

  // File creation params
  FILE_CREATE_FID,
  FILE_CREATE_FXID,
  FILE_CREATE_EOS_BTIME,
  FILE_CREATE_ARCHIVE_METADATA,

  // sec.app - Used to classify EOS report log messages
  // Should be last, by convention
  SEC_APP,
};

// Base class for EOS-CTA Report
// Most logic is implemented here
class Reporter
{
public:

  virtual ~Reporter()
  {
    if (mActive) {
      generateEosReportEntry();
    }
  }

  template<typename T>
  Reporter& addParam(ReportParam key, const T& val)
  {
    mParams[key] = std::to_string(val);
    return *this;
  }

  Reporter& addParam(ReportParam key, const std::string& val)
  {
    mParams[key] = val;
    return *this;
  }

  Reporter& addParam(ReportParam key, const bool& val)
  {
    mParams[key] = (val ? "true" : "false");
    return *this;
  }

  Reporter& addParam(ReportParam key, const char* val)
  {
    mParams[key] = val;
    return *this;
  }

protected:
  // EosCtaReporter class should not be used directly
  Reporter(std::function<void(const std::string&)> func = nullptr);
  Reporter(const Reporter& c) = delete;
  Reporter(Reporter&& c) : mParams(std::move(c.mParams)),
    mWriterCallback(std::move(c.mWriterCallback)), mActive(c.mActive)
  {
    c.mActive = false;
  }
  Reporter& operator=(const Reporter&) = delete;
  Reporter& operator=(Reporter&& c) = delete;

  std::map<ReportParam, std::string> mParams;

private:
  void generateEosReportEntry(); // It will be called during destruction of EosCtaReporter

  std::function<void(const std::string&)> mWriterCallback;
  bool mActive;
  static std::vector<ReportParam> DEFAULT_PARAMS;
};

// Prepare request EOS-CTA Reporter
class ReporterPrepareReq : public Reporter
{
public:
  // Prepare manager uses an interface to interact with the file system
  // This is the reason why we need to pass a log writer callback
  ReporterPrepareReq(std::function<void(const std::string&)> writeCallback);
private:
  static std::vector<ReportParam> DEFAULT_PARAMS_PREPARE_REQ;
};

// Prepare WFE EOS-CTA Reporter
class ReporterPrepareWfe : public Reporter
{
public:
  ReporterPrepareWfe();
private:
  static std::vector<ReportParam> DEFAULT_PARAMS_PREPARE_WFE;
};

// Evict cmd EOS-CTA Reporter
class ReporterEvict : public Reporter
{
public:
  ReporterEvict();
private:
  static std::vector<ReportParam> DEFAULT_PARAMS_EVICTCMD;
};

// File deletion EOS-CTA Reporter
class ReporterFileDeletion : public Reporter
{
public:
  ReporterFileDeletion();
private:
  static std::vector<ReportParam> DEFAULT_PARAMS_FILE_DELETION;
};

// File creation EOS-CTA Reporter
class ReporterFileCreation : public Reporter
{
public:
  ReporterFileCreation();
private:
  static std::vector<ReportParam> DEFAULT_PARAMS_FILE_CREATION;
};

} // namespace cta


