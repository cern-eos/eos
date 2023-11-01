// ----------------------------------------------------------------------
// File: Iolimit.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
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

#include "common/Constants.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"
#include "common/Path.hh"
#include "common/utils/BackOffInvoker.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "common/table_formatter/TableFormatting.hh"
#include "mgm/Iolimit.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/Stat.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Run asynchronous Iolimit thread
//------------------------------------------------------------------------------

bool
Iolimit::Start()
{
  mThread.reset(&Iolimit::Computer, this);
  return true;
}

//------------------------------------------------------------------------------
// Cancel the asynchronous Iolimit thread
//------------------------------------------------------------------------------
void
Iolimit::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Eternal thread grabbing and computing IO performance scalers
//------------------------------------------------------------------------------
void
Iolimit::Computer(ThreadAssistant& assistant) noexcept
{
  eos_static_info("msg=\"Iolimt regulation thread started\"");
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  if (assistant.terminationRequested()) {
    return;
  }

  assistant.wait_for(std::chrono::seconds(15));
  eos::common::BackOffInvoker backoff_logger;

  while (!assistant.terminationRequested()) {
    // Every now and then we wake up
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    readLimits();
    readCurrent();
    computeScaler();
  }
  eos_static_info("msg=\"Iolimt regulation thread exiting\"");
}

//------------------------------------------------------------------------------
// Read defined limits
//------------------------------------------------------------------------------
void
Iolimit::readLimits()
{
}


//------------------------------------------------------------------------------
// Read current usage
//------------------------------------------------------------------------------
void
Iolimit::readCurrent()
{
  std::string z64json;
  std::string json;
  std::map<std::string, std::map<std::string, double>> data;

  {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    for (auto it = FsView::gFsView.mNodeView.begin(); it != FsView::gFsView.mNodeView.end(); ++it) {
      z64json = it->second->GetMember("cfg.stat.iotop.z64");
      bool done = eos::common::SymKey::ZDeBase64(z64json, json);
      if (done) {
	eos_static_debug("node='%s' json='%s'", it->first.c_str(), json.c_str());
	Json::Value js;
	std::stringstream sstr(json);
	time_t now = time(NULL);
	try {
	  sstr >> js;
	  auto created=js["publishing"]["unixtime"].asUInt64();
	  
	  if ( (now - created) > 60) {
	    eos_static_debug("msg=\"discarding report\" age=%lu seconds", now-created);
	    continue;
	  }
	  
	  // Get the values
	  const Json::Value values = js["activity"];
	  
	  for(size_t j = 0; j < values.getMemberNames().size(); j++) {
	    std::string key = values.getMemberNames()[j];
	    for (size_t k = 0; k < values[key].getMemberNames().size(); k++) {
	      std::string ikey = values[key].getMemberNames()[k];
	      auto val = values[key][ikey].asDouble();
	      data[key][ikey] += val;
	    }
	  }
	} catch (...) {
	}
      }
    }
  }

  std::map<std::string, double> nextCurrent;
  {
    // let's convert the data into single key=>value
    for ( auto x = data.begin(); x != data.end();++x) {
      for ( auto y = x->second.begin(); y != x->second.end(); ++y) {
	std::string key = x->first;
	key += ":";
	key += y->first;
	nextCurrent[key] = y->second;
      }
    }
  }
  {
    std::lock_guard l(currentMutex);
    idCurrent = nextCurrent;
  }
}

//------------------------------------------------------------------------------
// Compute scalers to apply
//------------------------------------------------------------------------------
void
Iolimit::computeScaler()
{
}

//------------------------------------------------------------------------------
// Print status
//------------------------------------------------------------------------------
std::string
Iolimit::Print(std::string filter, std::string rangefilter, std::string keyfilter)
{
  std::string out;
  TableFormatterBase table;
  TableData body;
  TableRow row;

  table.SetHeader({
      {"type", 4, "s"},
      {"id", 8, "s"},
      {"key", 12, "s"},
      {"range", 6, "s"},
      {"current", 12, "+f"},
      {"limit",   12, "+f"},
      {"scaler",  12, "+f"}
    });

  for (auto it=idCurrent.begin(); it!=idCurrent.end(); ++it) {
    TableRow row;
    std::vector<std::string> tokens;
    std::string type;
    std::string id;
    std::string key;
    std::string range;
    double val = it->second;
    // split key by ':' type:id:counter:range
    eos::common::StringConversion::Tokenize(it->first, tokens, ":");
    if (tokens.size() != 4) {
      continue;
    }
    type  = tokens[0];
    id    = tokens[1];
    if ( type == "uid") {
      int errc=0;
      id = eos::common::Mapping::UidToUserName(std::strtoul(tokens[1].c_str(),0,10),errc);
    }
    if ( type == "gid") {
      int errc=0;
      id = eos::common::Mapping::GidToGroupName(std::strtoul(tokens[1].c_str(),0,10),errc);
    }
    key   = tokens[2];
    range = tokens[3];

    // listing filter

    if (filter.length() && filter != type) {
      continue;
    }
    if (rangefilter.length() && range != rangefilter) {
      continue;
    }
    if (keyfilter.length() && key.find(keyfilter)==std::string::npos) {
      continue;
    }

    if (range == "sum") {
      // useless for now
      continue;
    }
    
    if ( (range == "exec_ms") ||
	 (range == "sigma_ms") ) {
      if ( (key != "rbytes") &&
	   (key != "wbytes") ) {
	continue;
      }
    }
    row.emplace_back(type, "s");
    row.emplace_back(id, "s");
    row.emplace_back(key, "s");
    row.emplace_back(range, "s");
    if ( (key != "rbytes" ) &&
	 (key != "wbytes" ) ) {
      row.emplace_back((unsigned long long)it->second, "l", "");
      row.emplace_back(0, "l", "");
    } else {
      if ( (range == "exec_ms") ||
	   (range == "sigma_ms") ) {
	row.emplace_back(it->second, "+f", "");
	row.emplace_back(0.1, "+f", "s");
      } else {
	row.emplace_back(it->second, "+f", "B/s");
	row.emplace_back(0, "+f", "B/s");
      }
    }
    row.emplace_back(0, "+f");
    body.push_back(row);
  }

  body.push_back(row);
  table.AddRows(body); 
  out += table.GenerateTable();
  return out;
}



EOSMGMNAMESPACE_END
