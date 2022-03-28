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
        {EosCtaReportParam::LOG                  , "log"                  },
        {EosCtaReportParam::PATH                 , "path"                 },
        {EosCtaReportParam::RUID                 , "ruid"                 },
        {EosCtaReportParam::RGID                 , "rgid"                 },
        {EosCtaReportParam::TD                   , "td"                   },
        {EosCtaReportParam::HOST                 , "host"                 },

        // Prepare request params
        {EosCtaReportParam::PREP_REQ_TS          , "prep.req.ts"          },
        {EosCtaReportParam::PREP_REQ_TNS         , "prep.req.tns"         },
        {EosCtaReportParam::PREP_REQ_EVENT       , "prep.req.event"       },
        {EosCtaReportParam::PREP_REQ_REQID       , "prep.req.reqid"       },
        {EosCtaReportParam::PREP_REQ_SENTTOWFE   , "prep.req.senttowfe"   },
        {EosCtaReportParam::PREP_REQ_SUCCESSFUL  , "prep.req.successful"  },
        {EosCtaReportParam::PREP_REQ_ERROR       , "prep.req.error"       },

        // WFE params
        {EosCtaReportParam::PREP_WFE_TS          , "prep.wfe.ts"          },
        {EosCtaReportParam::PREP_WFE_TNS         , "prep.wfe.tns"         },
        {EosCtaReportParam::PREP_WFE_EVENT       , "prep.wfe.event"       },
        {EosCtaReportParam::PREP_WFE_REQID       , "prep.wfe.reqid"       },
        {EosCtaReportParam::PREP_WFE_REQCOUNT    , "prep.wfe.reqcount"    },
        {EosCtaReportParam::PREP_WFE_REQLIST     , "prep.wfe.reqlist"     },
        {EosCtaReportParam::PREP_WFE_EVICTCOUNTER, "prep.wfe.evictcounter"},
        {EosCtaReportParam::PREP_WFE_ONDISK      , "prep.wfe.ondisk"      },
        {EosCtaReportParam::PREP_WFE_ONTAPE      , "prep.wfe.ontape"      },
        {EosCtaReportParam::PREP_WFE_FIRSTPREPARE, "prep.wfe.firstprepare"},
        {EosCtaReportParam::PREP_WFE_SENTTOCTA   , "prep.wfe.senttocta"   },
        {EosCtaReportParam::PREP_WFE_ACTIVITY    , "prep.wfe.activity"    },
        {EosCtaReportParam::PREP_WFE_ERROR       , "prep.req.error"       },

        // StagerRm params
        {EosCtaReportParam::STAGERRM_TS          , "stagerrm.ts"          },
        {EosCtaReportParam::STAGERRM_TNS         , "stagerrm.tns"         },
        {EosCtaReportParam::STAGERRM_EVICTCOUNTER, "stagerrm.evictcounter"},
        {EosCtaReportParam::STAGERRM_FILEREMOVED , "stagerrm.fileremoved" },
        {EosCtaReportParam::STAGERRM_ERROR       , "stagerrm.error"       },
};

// Basic mParams
std::vector<EosCtaReportParam> EosCtaReporter::DEFAULT_PARAMS{
        EosCtaReportParam::LOG,
        EosCtaReportParam::PATH,
        EosCtaReportParam::RUID,
        EosCtaReportParam::RGID,
        EosCtaReportParam::TD,
        EosCtaReportParam::HOST,
};

// Prepare request mParams
std::vector<EosCtaReportParam> EosCtaReporterPrepareReq::DEFAULT_PARAMS_PREPARE_REQ{
        EosCtaReportParam::PREP_REQ_TS,
        EosCtaReportParam::PREP_REQ_TNS,
        EosCtaReportParam::PREP_REQ_EVENT,
        EosCtaReportParam::PREP_REQ_REQID,
        EosCtaReportParam::PREP_REQ_SENTTOWFE,
        EosCtaReportParam::PREP_REQ_SUCCESSFUL,
        EosCtaReportParam::PREP_REQ_ERROR,
};

// WFE mParams
std::vector<EosCtaReportParam> EosCtaReporterPrepareWfe::DEFAULT_PARAMS_PREPARE_WFE{
        EosCtaReportParam::PREP_WFE_TS,
        EosCtaReportParam::PREP_WFE_TNS,
        EosCtaReportParam::PREP_WFE_EVENT,
        EosCtaReportParam::PREP_WFE_REQID,
        EosCtaReportParam::PREP_WFE_REQCOUNT,
        EosCtaReportParam::PREP_WFE_REQLIST,
        EosCtaReportParam::PREP_WFE_EVICTCOUNTER,
        EosCtaReportParam::PREP_WFE_ONDISK,
        EosCtaReportParam::PREP_WFE_ONTAPE,
        EosCtaReportParam::PREP_WFE_FIRSTPREPARE,
        EosCtaReportParam::PREP_WFE_SENTTOCTA,
        EosCtaReportParam::PREP_WFE_ACTIVITY,
        EosCtaReportParam::PREP_WFE_ERROR,
};

// StagerRm mParams
std::vector<EosCtaReportParam> EosCtaReporterStagerRm::DEFAULT_PARAMS_STAGERRM{
        EosCtaReportParam::STAGERRM_TS,
        EosCtaReportParam::STAGERRM_TNS,
        EosCtaReportParam::STAGERRM_EVICTCOUNTER,
        EosCtaReportParam::STAGERRM_FILEREMOVED,
        EosCtaReportParam::STAGERRM_ERROR,
};

// Default function used to write the EOS-CTA reports
void ioStatsWrite(const std::string & in) {
  if(gOFS->IoStats) {
    gOFS->IoStats->WriteRecord(in);
  }
}

void EosCtaReporter::generateEosReportEntry() {
  std::ostringstream oss;
  for(auto it = mParams.begin(); it != mParams.end(); it++) {
    if (it != mParams.begin()) {
      oss << "&";
    }
    oss << EosCtaParamMap.at(it->first) << "=" << it->second;
  }
  mWriterCallback(oss.str());
}

EosCtaReporter::EosCtaReporter(std::function<void(const std::string&)> writeCallback) : mActive(true) {
  mWriterCallback = writeCallback ? writeCallback : &ioStatsWrite;
  for (auto key : DEFAULT_PARAMS) {
    mParams[key] = "";
  }
}

EosCtaReporterPrepareReq::EosCtaReporterPrepareReq(std::function<void(const std::string&)> func) : EosCtaReporter(func) {
  for (auto key : DEFAULT_PARAMS_PREPARE_REQ) {
    mParams[key] = "";
  }
}

EosCtaReporterPrepareWfe::EosCtaReporterPrepareWfe() {
  for (auto key : DEFAULT_PARAMS_PREPARE_WFE) {
    mParams[key] = "";
  }
}

EosCtaReporterStagerRm::EosCtaReporterStagerRm() {
  for (auto key : DEFAULT_PARAMS_STAGERRM) {
    mParams[key] = "";
  }
}

EOSMGMNAMESPACE_END