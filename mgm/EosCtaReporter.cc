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

#include "mgm/EosCtaReporter.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Iostat.hh"

EOSMGMNAMESPACE_BEGIN

static const std::map<EosCtaReportParam, std::string> EosCtaParamMap{
  // Basic params
  {EosCtaReportParam::LOG, "log"    },
  {EosCtaReportParam::PATH, "path"   },
  {EosCtaReportParam::RUID, "ruid"   },
  {EosCtaReportParam::RGID, "rgid"   },
  {EosCtaReportParam::TD, "td"     },
  {EosCtaReportParam::HOST, "host"   },
  {EosCtaReportParam::TS, "ts"     },
  {EosCtaReportParam::TNS, "tns"    },
  {EosCtaReportParam::SEC_APP, "sec.app"}, // sec.app - Used to classify EOS report log messages

  // Prepare request params
  {EosCtaReportParam::PREP_REQ_EVENT, "event"     },
  {EosCtaReportParam::PREP_REQ_REQID, "reqid"     },
  {EosCtaReportParam::PREP_REQ_SENTTOWFE, "senttowfe" },
  {EosCtaReportParam::PREP_REQ_SUCCESSFUL, "successful"},
  {EosCtaReportParam::PREP_REQ_ERROR, "error"     },

  // WFE params
  {EosCtaReportParam::PREP_WFE_EVENT, "event"       },
  {EosCtaReportParam::PREP_WFE_REQID, "reqid"       },
  {EosCtaReportParam::PREP_WFE_REQCOUNT, "reqcount"    },
  {EosCtaReportParam::PREP_WFE_EVICTCOUNTER, "evictcounter"},
  {EosCtaReportParam::PREP_WFE_ONDISK, "ondisk"      },
  {EosCtaReportParam::PREP_WFE_ONTAPE, "ontape"      },
  {EosCtaReportParam::PREP_WFE_FIRSTPREPARE, "firstprepare"},
  {EosCtaReportParam::PREP_WFE_SENTTOCTA, "senttocta"   },
  {EosCtaReportParam::PREP_WFE_ACTIVITY, "activity"    },
  {EosCtaReportParam::PREP_WFE_ERROR, "error"       },

  // Evict cmd params
  {EosCtaReportParam::EVICTCMD_EVICTCOUNTER, "evictcounter"},
  {EosCtaReportParam::EVICTCMD_FILEREMOVED, "fileremoved" },
  {EosCtaReportParam::EVICTCMD_ERROR, "error"       },
  {EosCtaReportParam::EVICTCMD_FSID, "fsid"        },

  // File deletion params
  {EosCtaReportParam::FILE_DEL_FID, "fid"                  },
  {EosCtaReportParam::FILE_DEL_FXID, "fxid"                 },
  {EosCtaReportParam::FILE_DEL_EOS_BTIME, "eos.btime"            },
  {EosCtaReportParam::FILE_DEL_ARCHIVE_FILE_ID, "archive.file_id"      },
  {EosCtaReportParam::FILE_DEL_ARCHIVE_STORAGE_CLASS, "archive.storage_class"},
  {EosCtaReportParam::FILE_DEL_LOCATIONS, "locations"            },
  {EosCtaReportParam::FILE_DEL_CHECKSUMTYPE, "checksumtype"         },
  {EosCtaReportParam::FILE_DEL_CHECKSUMVALUE, "checksumvalue"        },
  {EosCtaReportParam::FILE_DEL_SIZE, "size"                 },

  // File creation params
  {EosCtaReportParam::FILE_CREATE_FID, "fid"                  },
  {EosCtaReportParam::FILE_CREATE_FXID, "fxid"                 },
  {EosCtaReportParam::FILE_CREATE_EOS_BTIME, "eos.btime"            },
  {EosCtaReportParam::FILE_CREATE_ARCHIVE_METADATA, "archivemetadata"      },
};

// Basic mParams
std::vector<EosCtaReportParam> EosCtaReporter::DEFAULT_PARAMS{
  EosCtaReportParam::LOG,
  EosCtaReportParam::PATH,
  EosCtaReportParam::RUID,
  EosCtaReportParam::RGID,
  EosCtaReportParam::TD,
  EosCtaReportParam::HOST,
  EosCtaReportParam::TS,
  EosCtaReportParam::TNS,
  EosCtaReportParam::SEC_APP,
};

// Prepare request mParams
std::vector<EosCtaReportParam>
EosCtaReporterPrepareReq::DEFAULT_PARAMS_PREPARE_REQ{
  EosCtaReportParam::PREP_REQ_EVENT,
  EosCtaReportParam::PREP_REQ_REQID,
  EosCtaReportParam::PREP_REQ_SENTTOWFE,
  EosCtaReportParam::PREP_REQ_SUCCESSFUL,
  EosCtaReportParam::PREP_REQ_ERROR,
};

// WFE mParams
std::vector<EosCtaReportParam>
EosCtaReporterPrepareWfe::DEFAULT_PARAMS_PREPARE_WFE{
  EosCtaReportParam::PREP_WFE_EVENT,
  EosCtaReportParam::PREP_WFE_REQID,
  EosCtaReportParam::PREP_WFE_REQCOUNT,
  EosCtaReportParam::PREP_WFE_EVICTCOUNTER,
  EosCtaReportParam::PREP_WFE_ONDISK,
  EosCtaReportParam::PREP_WFE_ONTAPE,
  EosCtaReportParam::PREP_WFE_FIRSTPREPARE,
  EosCtaReportParam::PREP_WFE_SENTTOCTA,
  EosCtaReportParam::PREP_WFE_ACTIVITY,
  EosCtaReportParam::PREP_WFE_ERROR,
};

// Evict cmd mParams
std::vector<EosCtaReportParam> EosCtaReporterEvict::DEFAULT_PARAMS_EVICTCMD{
  EosCtaReportParam::EVICTCMD_EVICTCOUNTER,
  EosCtaReportParam::EVICTCMD_FILEREMOVED,
  EosCtaReportParam::EVICTCMD_ERROR,
};

// File deletion mParams
std::vector<EosCtaReportParam>
EosCtaReporterFileDeletion::DEFAULT_PARAMS_FILE_DELETION{
  EosCtaReportParam::FILE_DEL_FID,
  EosCtaReportParam::FILE_DEL_FXID,
  EosCtaReportParam::FILE_DEL_EOS_BTIME,
  EosCtaReportParam::FILE_DEL_ARCHIVE_FILE_ID,
  EosCtaReportParam::FILE_DEL_ARCHIVE_STORAGE_CLASS,
  EosCtaReportParam::FILE_DEL_LOCATIONS,
  EosCtaReportParam::FILE_DEL_CHECKSUMTYPE,
  EosCtaReportParam::FILE_DEL_CHECKSUMVALUE,
  EosCtaReportParam::FILE_DEL_SIZE,
};

// File creation mParams
std::vector<EosCtaReportParam>
EosCtaReporterFileCreation::DEFAULT_PARAMS_FILE_CREATION{
  EosCtaReportParam::FILE_CREATE_FID,
  EosCtaReportParam::FILE_CREATE_FXID,
  EosCtaReportParam::FILE_CREATE_EOS_BTIME,
  EosCtaReportParam::FILE_CREATE_ARCHIVE_METADATA,
};

// Default function used to write the EOS-CTA reports
void ioStatsWrite(const std::string& in)
{
  if (gOFS->mIoStats) {
    gOFS->mIoStats->WriteRecord(in);
  }
}

void EosCtaReporter::generateEosReportEntry()
{
  std::ostringstream oss;

  for (auto it = mParams.begin(); it != mParams.end(); it++) {
    if (it != mParams.begin()) {
      oss << "&";
    }

    oss << EosCtaParamMap.at(it->first) << "=" << it->second;
  }

  mWriterCallback(oss.str());
}

EosCtaReporter::EosCtaReporter(std::function<void(const std::string&)>
                               writeCallback) : mActive(true)
{
  mWriterCallback = writeCallback ? writeCallback : &ioStatsWrite;

  for (auto key : DEFAULT_PARAMS) {
    mParams[key] = "";
  }
}

EosCtaReporterPrepareReq::EosCtaReporterPrepareReq(
  std::function<void(const std::string&)> func) : EosCtaReporter(func)
{
  for (auto key : DEFAULT_PARAMS_PREPARE_REQ) {
    mParams[key] = "";
  }
}

EosCtaReporterPrepareWfe::EosCtaReporterPrepareWfe()
{
  for (auto key : DEFAULT_PARAMS_PREPARE_WFE) {
    mParams[key] = "";
  }
}

EosCtaReporterEvict::EosCtaReporterEvict()
{
  for (auto key : DEFAULT_PARAMS_EVICTCMD) {
    mParams[key] = "";
  }
}

EosCtaReporterFileDeletion::EosCtaReporterFileDeletion()
{
  for (auto key : DEFAULT_PARAMS_FILE_DELETION) {
    mParams[key] = "";
  }
}

EosCtaReporterFileCreation::EosCtaReporterFileCreation()
{
  for (auto key : DEFAULT_PARAMS_FILE_CREATION) {
    mParams[key] = "";
  }
}

EOSMGMNAMESPACE_END
