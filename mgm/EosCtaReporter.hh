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

#include "mgm/Namespace.hh"

EOSMGMNAMESPACE_BEGIN

// All EOS-CTA Report fields should be listed here
enum class EosCtaReportParam {
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
  PREP_WFE_REQLIST,
  PREP_WFE_EVICTCOUNTER,
  PREP_WFE_ONDISK,
  PREP_WFE_ONTAPE,
  PREP_WFE_FIRSTPREPARE,
  PREP_WFE_SENTTOCTA,
  PREP_WFE_ACTIVITY,
  PREP_WFE_ERROR,

  // StagerRm params
  STAGERRM_EVICTCOUNTER,
  STAGERRM_FILEREMOVED,
  STAGERRM_ERROR,

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
  FILE_DEL_SEC_APP,
};

// Base class for EOS-CTA Report
// Most logic is implemented here
class EosCtaReporter {
public:

  virtual ~EosCtaReporter() {
    if(mActive) {
      generateEosReportEntry();
    }
  }

  template<typename T>
  EosCtaReporter & addParam(EosCtaReportParam key, const T& val) {
    mParams[key] = std::to_string(val);
    return *this;
  }

  EosCtaReporter & addParam(EosCtaReportParam key, const std::string& val) {
    mParams[key] = val;
    return *this;
  }

  EosCtaReporter & addParam(EosCtaReportParam key, const bool& val) {
    mParams[key] = (val ? "true" : "false");
    return *this;
  }

  EosCtaReporter & addParam(EosCtaReportParam key, const char* val) {
    mParams[key] = val;
    return *this;
  }

protected:
  // EosCtaReporter class should not be used directly
  EosCtaReporter(std::function<void(const std::string&)> func= nullptr);
  EosCtaReporter(const EosCtaReporter &c)=delete;
  EosCtaReporter(EosCtaReporter &&c) : mParams(std::move(c.mParams)), mWriterCallback(std::move(c.mWriterCallback)), mActive(c.mActive) { c.mActive = false; }
  EosCtaReporter& operator=(const EosCtaReporter&)=delete;
  EosCtaReporter& operator=(EosCtaReporter &&c)=delete;

  std::map<EosCtaReportParam, std::string> mParams;

private:
  void generateEosReportEntry(); // It will be called during destruction of EosCtaReporter

  std::function<void(const std::string&)> mWriterCallback;
  bool mActive;
  static std::vector<EosCtaReportParam> DEFAULT_PARAMS;
};

// Prepare request EOS-CTA Reporter
class EosCtaReporterPrepareReq : public EosCtaReporter {
public:
  // Prepare manager uses an interface to interact with the file system
  // This is the reason why we need to pass a log writer callback
  EosCtaReporterPrepareReq(std::function<void(const std::string&)> writeCallback);
private:
  static std::vector<EosCtaReportParam> DEFAULT_PARAMS_PREPARE_REQ;
};

// Prepare WFE EOS-CTA Reporter
class EosCtaReporterPrepareWfe : public EosCtaReporter {
public:
  EosCtaReporterPrepareWfe();
private:
  static std::vector<EosCtaReportParam> DEFAULT_PARAMS_PREPARE_WFE;
};

// StagerRm EOS-CTA Reporter
class EosCtaReporterStagerRm : public EosCtaReporter {
public:
  EosCtaReporterStagerRm();
private:
  static std::vector<EosCtaReportParam> DEFAULT_PARAMS_STAGERRM;
};

// File deletion EOS-CTA Reporter
class EosCtaReporterFileDeletion : public EosCtaReporter {
public:
  EosCtaReporterFileDeletion();
private:
  static std::vector<EosCtaReportParam> DEFAULT_PARAMS_FILE_DELETION;
};

EOSMGMNAMESPACE_END