//------------------------------------------------------------------------------
// File: EosCtaReporter.cc
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

#include <functional>
#include <sstream>
#include <map>

#include "mgm/cta/Reporter.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Iostat.hh"

namespace cta {

static const std::map<ReportParam, std::string> ReportParamMap{
  // Basic params
  {ReportParam::LOG, "log"    },
  {ReportParam::PATH, "path"   },
  {ReportParam::RUID, "ruid"   },
  {ReportParam::RGID, "rgid"   },
  {ReportParam::TD, "td"     },
  {ReportParam::HOST, "host"   },
  {ReportParam::TS, "ts"     },
  {ReportParam::TNS, "tns"    },
  {ReportParam::SEC_APP, "sec.app"}, // sec.app - Used to classify EOS report log messages

  // Prepare request params
  {ReportParam::PREP_REQ_EVENT, "event"     },
  {ReportParam::PREP_REQ_REQID, "reqid"     },
  {ReportParam::PREP_REQ_SENTTOWFE, "senttowfe" },
  {ReportParam::PREP_REQ_SUCCESSFUL, "successful"},
  {ReportParam::PREP_REQ_ERROR, "error"     },

  // WFE params
  {ReportParam::PREP_WFE_EVENT, "event"       },
  {ReportParam::PREP_WFE_REQID, "reqid"       },
  {ReportParam::PREP_WFE_REQCOUNT, "reqcount"    },
  {ReportParam::PREP_WFE_EVICTCOUNTER, "evictcounter"},
  {ReportParam::PREP_WFE_ONDISK, "ondisk"      },
  {ReportParam::PREP_WFE_ONTAPE, "ontape"      },
  {ReportParam::PREP_WFE_FIRSTPREPARE, "firstprepare"},
  {ReportParam::PREP_WFE_SENTTOCTA, "senttocta"   },
  {ReportParam::PREP_WFE_ACTIVITY, "activity"    },
  {ReportParam::PREP_WFE_ERROR, "error"       },

  // Evict cmd params
  {ReportParam::EVICTCMD_EVICTCOUNTER, "evictcounter"},
  {ReportParam::EVICTCMD_FILEREMOVED, "fileremoved" },
  {ReportParam::EVICTCMD_ERROR, "error"       },
  {ReportParam::EVICTCMD_FSID, "fsid"        },

  // File deletion params
  {ReportParam::FILE_DEL_FID, "fid"                  },
  {ReportParam::FILE_DEL_FXID, "fxid"                 },
  {ReportParam::FILE_DEL_EOS_BTIME, "eos.btime"            },
  {ReportParam::FILE_DEL_ARCHIVE_FILE_ID, "archive.file_id"      },
  {ReportParam::FILE_DEL_ARCHIVE_STORAGE_CLASS, "archive.storage_class"},
  {ReportParam::FILE_DEL_LOCATIONS, "locations"            },
  {ReportParam::FILE_DEL_CHECKSUMTYPE, "checksumtype"         },
  {ReportParam::FILE_DEL_CHECKSUMVALUE, "checksumvalue"        },
  {ReportParam::FILE_DEL_SIZE, "size"                 },

  // File creation params
  {ReportParam::FILE_CREATE_FID, "fid"                  },
  {ReportParam::FILE_CREATE_FXID, "fxid"                 },
  {ReportParam::FILE_CREATE_EOS_BTIME, "eos.btime"            },
  {ReportParam::FILE_CREATE_ARCHIVE_METADATA, "archivemetadata"      },
};

// Basic mParams
std::vector<ReportParam> Reporter::DEFAULT_PARAMS{
  ReportParam::LOG,
  ReportParam::PATH,
  ReportParam::RUID,
  ReportParam::RGID,
  ReportParam::TD,
  ReportParam::HOST,
  ReportParam::TS,
  ReportParam::TNS,
  ReportParam::SEC_APP,
};

// Prepare request mParams
std::vector<ReportParam>
ReporterPrepareReq::DEFAULT_PARAMS_PREPARE_REQ{
  ReportParam::PREP_REQ_EVENT,
  ReportParam::PREP_REQ_REQID,
  ReportParam::PREP_REQ_SENTTOWFE,
  ReportParam::PREP_REQ_SUCCESSFUL,
  ReportParam::PREP_REQ_ERROR,
};

// WFE mParams
std::vector<ReportParam>
ReporterPrepareWfe::DEFAULT_PARAMS_PREPARE_WFE{
  ReportParam::PREP_WFE_EVENT,
  ReportParam::PREP_WFE_REQID,
  ReportParam::PREP_WFE_REQCOUNT,
  ReportParam::PREP_WFE_EVICTCOUNTER,
  ReportParam::PREP_WFE_ONDISK,
  ReportParam::PREP_WFE_ONTAPE,
  ReportParam::PREP_WFE_FIRSTPREPARE,
  ReportParam::PREP_WFE_SENTTOCTA,
  ReportParam::PREP_WFE_ACTIVITY,
  ReportParam::PREP_WFE_ERROR,
};

// Evict cmd mParams
std::vector<ReportParam> ReporterEvict::DEFAULT_PARAMS_EVICTCMD{
  ReportParam::EVICTCMD_EVICTCOUNTER,
  ReportParam::EVICTCMD_FILEREMOVED,
  ReportParam::EVICTCMD_ERROR,
};

// File deletion mParams
std::vector<ReportParam>
ReporterFileDeletion::DEFAULT_PARAMS_FILE_DELETION{
  ReportParam::FILE_DEL_FID,
  ReportParam::FILE_DEL_FXID,
  ReportParam::FILE_DEL_EOS_BTIME,
  ReportParam::FILE_DEL_ARCHIVE_FILE_ID,
  ReportParam::FILE_DEL_ARCHIVE_STORAGE_CLASS,
  ReportParam::FILE_DEL_LOCATIONS,
  ReportParam::FILE_DEL_CHECKSUMTYPE,
  ReportParam::FILE_DEL_CHECKSUMVALUE,
  ReportParam::FILE_DEL_SIZE,
};

// File creation mParams
std::vector<ReportParam>
ReporterFileCreation::DEFAULT_PARAMS_FILE_CREATION{
  ReportParam::FILE_CREATE_FID,
  ReportParam::FILE_CREATE_FXID,
  ReportParam::FILE_CREATE_EOS_BTIME,
  ReportParam::FILE_CREATE_ARCHIVE_METADATA,
};

// Default function used to write the EOS-CTA reports
static void ioStatsWrite(const std::string& in)
{
  if (gOFS && gOFS->mIoStats) {
    gOFS->mIoStats->WriteRecord(in);
  }
}

void Reporter::generateEosReportEntry()
{
  std::ostringstream oss;

  for (auto it = mParams.begin(); it != mParams.end(); it++) {
    if (it != mParams.begin()) {
      oss << "&";
    }

    oss << ReportParamMap.at(it->first) << "=" << it->second;
  }

  mWriterCallback(oss.str());
}

Reporter::Reporter(std::function<void(const std::string&)>
                   writeCallback) : mActive(true)
{
  mWriterCallback = writeCallback ? writeCallback : &ioStatsWrite;

  for (auto key : DEFAULT_PARAMS) {
    mParams[key] = "";
  }
}

ReporterPrepareReq::ReporterPrepareReq(
  std::function<void(const std::string&)> func) : Reporter(func)
{
  for (auto key : DEFAULT_PARAMS_PREPARE_REQ) {
    mParams[key] = "";
  }
}

ReporterPrepareWfe::ReporterPrepareWfe()
{
  for (auto key : DEFAULT_PARAMS_PREPARE_WFE) {
    mParams[key] = "";
  }
}

ReporterEvict::ReporterEvict()
{
  for (auto key : DEFAULT_PARAMS_EVICTCMD) {
    mParams[key] = "";
  }
}

ReporterFileDeletion::ReporterFileDeletion()
{
  for (auto key : DEFAULT_PARAMS_FILE_DELETION) {
    mParams[key] = "";
  }
}

ReporterFileCreation::ReporterFileCreation()
{
  for (auto key : DEFAULT_PARAMS_FILE_CREATION) {
    mParams[key] = "";
  }
}

} // namespace cta


