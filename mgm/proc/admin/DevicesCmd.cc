//------------------------------------------------------------------------------
// @file: DevicesCmd.cc
// @author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include "DevicesCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/tgc/Constants.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/devices/Devices.hh"
#include "common/Path.hh"
#include "mgm/config/IConfigEngine.hh"
#include "common/Constants.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "common/SymKeys.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "common/table_formatter/TableFormatting.hh"

EOSMGMNAMESPACE_BEGIN
//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
DevicesCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::DevicesProto devices = mReqProto.devices();

  switch (mReqProto.devices().subcmd_case()) {
  case eos::console::DevicesProto::kLs:
    LsSubcmd(devices.ls(), reply);
    break;

  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
  }

  return reply;
}

//----------------------------------------------------------------------------
// Execute ls subcommand
//----------------------------------------------------------------------------
void DevicesCmd::LsSubcmd(const eos::console::DevicesProto_LsProto& ls,
                          eos::console::ReplyProto& reply)
{
  using eos::console::DevicesProto;
  std::string list_format;
  std::string format;
  auto format_case = ls.outformat();
  Json::Value gjson;

  if ((format_case == DevicesProto::LsProto::NONE) && WantsJsonOutput()) {
    format_case = DevicesProto::LsProto::MONITORING;
  }

  switch (format_case) {
  case DevicesProto::LsProto::LISTING:
    break;

  case DevicesProto::LsProto::MONITORING:
    break;

  default : // NONE
    break;
  }

  std::string std_out;

  if (ls.refresh()) {
    //  force a new extraction
    gOFS->mDeviceTracker->Extract();
  }

  auto extractionTime = gOFS->mDeviceTracker->getExtractionTime();
  auto extractionLocalTime = gOFS->mDeviceTracker->getLocalExtractionTime();

  if (format_case != DevicesProto::LsProto::MONITORING) {
    std_out += "# ";
    std_out += extractionLocalTime;
    std_out += "\n";
  }

  for (auto it = FsView::gFsView.mSpaceView.begin();
       it != FsView::gFsView.mSpaceView.end(); ++it) {
    std::map<std::string, std::map<std::string, uint64_t>> driveModelStats;
    std::map<std::string, std::map<std::string, uint64_t>> driveModelSmartStats;
    double totalcapacity = 0;
    double totalhours = 0;
    double totaltbhours = 0;
    uint64_t totaldrivecount = 0;
    TableFormatterBase table;
    std::string space = it->first;

    if (format_case == DevicesProto::LsProto::MONITORING) {
      table.SetHeader({
        {"key", 0, "os"},
        {"space", 5, "os"},
        {"id", 5, "l"},
        {"model", 0, "-s"},
        {"serial", 0, "-s"},
        {"type", 0, "-s"},
        {"capacity", 0, "l"},
        {"rpms", 0, "l"},
        {"poweronhours", 0, "l"},
        {"temp", 0, "l"},
        {"smart", 0, "s"},
        {"if", 0, "-s"},
        {"rla", 0, "-s"},
        {"wc", 0, "-s"}
      });
    } else {
      table.SetHeader({
        {it->first, 12, "+l"},
        {"model", 0, "-s"},
        {"serial", 0, "-s"},
        {"type", 0, "-s"},
        {"capacity", 0, "+l"},
        {"rpms", 0, "l"},
        {"poweron[h]", 0, "l"},
        {"temp[degrees]", 0, "l"},
        {"S.M.A.R.T", 0, "s"},
        {"if", 0, "-s"},
        {"rla", 0, "-s"},
        {"wc", 0, "-s"}
      });
    }

    auto jinfo = gOFS->mDeviceTracker->getJson();
    auto spinfo = gOFS->mDeviceTracker->getSpaceMap();
    auto sminfo = gOFS->mDeviceTracker->getSmartMap();
    std::vector<std::string> smStatus {"OK", "no smartctl", "N/A", "FAILING", "Check", "invalid", "unknown"};
    std::vector<std::string> smHuman {"ok", "noctl", "na", "failing", "check", "inval", "unknown"};

    if (!jinfo || !spinfo) {
      if (!WantsJsonOutput()) {
        reply.set_std_err("error: not yet availabe - try again");
        reply.set_retc(EAGAIN);
      } else {
        reply.set_std_err("{ \"errmsg\" : \"not yet available -try again\", \"errc\" : 11 }");
        reply.set_retc(EAGAIN);
      }

      return ;
    }

    for (auto it = jinfo->begin(); it != jinfo->end(); ++it) {
      std::string smartstatus = "unknown";
      auto id = it->first;

      if (!spinfo->count(id)) {
        // fs not mapped to a space
        continue;
      }

      if (space != (*spinfo)[id]) {
        // no in this printout
        continue;
      }

      auto sm = sminfo->find(id);

      if (sm != sminfo->end()) {
        smartstatus = sm->second;
      }

      Json::Value root;
      std::string errs;
      Json::CharReaderBuilder jsonReaderBuilder;
      std::unique_ptr<Json::CharReader> const reader(
        jsonReaderBuilder.newCharReader());
      const std::string& ojson = it->second;

      if (reader->parse(ojson.c_str(), ojson.c_str() + ojson.size(), &root, &errs)) {
        try {
          std::string model     = root.isMember("model_name") ?
                                  root["model_name"].asString() : "unknown";
          std::replace(model.begin(), model.end(), ' ', ':');
          std::string serial     = root.isMember("serial_number") ?
                                   root["serial_number"].asString() : "unknown";
          std::string dtype     = (root.isMember("device") &&
                                   root["device"].isMember("type")) ? root["device"]["type"].asString() :
                                  "unknown";
          uint64_t capacity     = (root.isMember("user_capacity") &&
                                   root["user_capacity"].isMember("bytes")) ?
                                  root["user_capacity"]["bytes"].asUInt64() : 0;
          uint64_t rpms         = (root.isMember("rotation_rate")) ?
                                  root["rotation_rate"].asUInt64() : 0;
          uint64_t powerhours   = (root.isMember("power_on_time") &&
                                   root["power_on_time"].isMember("hours")) ?
                                  root["power_on_time"]["hours"].asUInt64() : 0;
          uint64_t temperature  = (root.isMember("temperature") &&
                                   root["temperature"].isMember("current")) ?
                                  root["temperature"]["current"].asUInt64() : 0;
          std::string ifspeed   = (root.isMember("interface_speed")
                                   && root["interface_speed"].isMember("max")
                                   && root["interface_speed"]["max"].isMember("string")) ?
                                  root["interface_speed"]["max"]["string"].asString() : "unknown";
          std::replace(ifspeed.begin(), ifspeed.end(), ' ', ':');
          std::string read_lookahead = (root.isMember("read_lookahead")) ?
                                       (root["read_lookahead"]["enabled"].asBool() ? "true" : "false") : "unknown";
          std::string write_cache    = (root.isMember("write_cache")) ?
                                       (root["write_cache"]["enabled"].asBool() ? "true" : "false") : "unknown";

          if (model.length()) {
            driveModelStats[model]["count"]++;
            totaldrivecount++;
            driveModelStats[model]["bytes"] += capacity;
            driveModelStats[model]["hours"] += powerhours;
            totalcapacity += capacity;
            totalhours += powerhours;
            totaltbhours += (capacity * powerhours / 1000000000000.0);

            if (!driveModelSmartStats.count(model)) {
              // make sure we have each smart status in the smart stats map
              for (size_t i = 0; i < smStatus.size(); ++i) {
                driveModelSmartStats[model][smHuman[i]] = 0;
              }
            }

            for (size_t i = 0; i < smStatus.size(); ++i) {
              if (sm->second == smStatus[i]) {
                smartstatus = smHuman[i];
                driveModelSmartStats[model][smartstatus]++;
                break;
              }
            }
          }

          TableData body;
          TableRow row;

          if (format_case == DevicesProto::LsProto::MONITORING) {
            row.emplace_back("deviceinfo", "os");
            row.emplace_back(space, "s");
          }

          row.emplace_back((unsigned long long)id, "l");
          row.emplace_back(model, "-s");
          row.emplace_back(serial, "-s");
          row.emplace_back(dtype, "-s");

          if (format_case == DevicesProto::LsProto::MONITORING) {
            row.emplace_back((unsigned long long)capacity, "l", "B");
          } else {
            row.emplace_back((unsigned long long)capacity, "+l", "B");
          }

          row.emplace_back((unsigned long long)rpms, "l");

          if (format_case == DevicesProto::LsProto::MONITORING) {
            row.emplace_back((unsigned long long)powerhours, "l");
          } else {
            row.emplace_back((unsigned long long)powerhours, "+l", "h");
          }

          row.emplace_back((unsigned long long)temperature, "l");
          row.emplace_back(smartstatus, "s");
          row.emplace_back(ifspeed, "-s");
          row.emplace_back(read_lookahead, "-s");
          row.emplace_back(write_cache, "-s");
          body.push_back(row);
          table.AddRows(body);
        } catch (Json::Exception const&) {
          std_out += "fatal: json exception has been thrown\n";
          eos_static_crit("err=\"catched JSON exception\"");
        }
      }

      gjson["extractiontime"]["timestamp"] = (Json::Value::UInt64)extractionTime;
      gjson["extractiontime"]["localtime"] = extractionLocalTime;
      gjson["space"][space]["filesystem"][std::to_string(id)] = root;
    }

    if (format_case == DevicesProto::LsProto::LISTING) {
      std_out += table.GenerateTable();
    }

    if (format_case == DevicesProto::LsProto::MONITORING) {
      std_out += table.GenerateTable();
    }

    if (true) { // we might add a switch to suppress this output later
      TableFormatterBase table;
      TableHeader header;
      TableData body;

      if (format_case == DevicesProto::LsProto::MONITORING) {
        header.push_back(std::make_tuple("key", 0, "os"));
        header.push_back(std::make_tuple("model", 0, "os"));
      } else {
        header.push_back(std::make_tuple("space", 0, "+s"));
        header.push_back(std::make_tuple("model", 0, "-s"));
      }

      auto fst = driveModelStats.begin();

      if (fst != driveModelStats.end()) {
        if (format_case == DevicesProto::LsProto::MONITORING) {
          header.push_back(std::make_tuple("avg:age:years", 0, "f"));
        } else {
          header.push_back(std::make_tuple("avg:age[years]", 0, "f"));
        }

        for (auto it = fst->second.begin(); it != fst->second.end(); ++it) {
          if (format_case == DevicesProto::LsProto::MONITORING) {
            header.push_back(std::make_tuple(it->first, 0, "l"));
          } else {
            header.push_back(std::make_tuple(it->first, 0, "+l"));
          }
        }
      }

      for (auto i = smHuman.begin(); i != smHuman.end(); ++i) {
        header.push_back(std::make_tuple(std::string("smrt:") + *i, 0, "os"));
      }

      table.SetHeader(header);

      for (auto it = driveModelStats.begin(); it != driveModelStats.end(); ++it) {
        double avgage = driveModelStats[it->first]["hours"] /
                        driveModelStats[it->first]["count"] / 24.0 / 365.0;
        TableRow row;

        if (format_case == DevicesProto::LsProto::MONITORING) {
          row.emplace_back("devicestats", "os");
        } else {
          row.emplace_back(space.c_str(), "s");
        }

        row.emplace_back(it->first, "-s");
        row.emplace_back(avgage, "f");
        gjson["statistics"][it->first]["avg:age:years"] = avgage;

        for (auto iit = it->second.begin(); iit != it->second.end(); ++iit) {
          if (format_case == DevicesProto::LsProto::MONITORING) {
            row.emplace_back((unsigned long long)iit->second, "l");
          } else {
            if (iit->first == "bytes") {
              row.emplace_back((unsigned long long)iit->second, "+l", "B");
            } else if (iit->first == "hours") {
              row.emplace_back((unsigned long long)iit->second, "+l", "h");
            } else {
              row.emplace_back((unsigned long long)iit->second, "+l");
            }
          }

          gjson["statistics"][it->first][iit->first] = (Json::Value::UInt64)iit->second;
        }

        for (size_t i = 0; i < smHuman.size(); ++i) {
          row.emplace_back((unsigned long long)
                           driveModelSmartStats[it->first][smHuman[i]], "l");
        }

        body.push_back(row);
      }

      table.AddRows(body);

      if (!WantsJsonOutput()) {
        std_out += table.GenerateTable();
      }
    }

    if (true) { // we might add a switch to suppress this output later
      TableFormatterBase table;
      TableHeader header;
      TableData body;

      if (format_case == DevicesProto::LsProto::MONITORING) {
        header.push_back(std::make_tuple("key", 0, "os"));
        header.push_back(std::make_tuple("tbyears", 0, "of"));
        header.push_back(std::make_tuple("driveage", 0, "of"));
        header.push_back(std::make_tuple("drivehours", 0, "ol"));
        header.push_back(std::make_tuple("clouddollar-replica", 0, "ol"));
        header.push_back(std::make_tuple("clouddollar-erasure", 0, "ol"));
      } else {
        header.push_back(std::make_tuple("Cost-Matrix", 0, "+s"));
        header.push_back(std::make_tuple("TB*Years", 0, "+l"));
        header.push_back(std::make_tuple("Avg-Drive-Hours", 6, "+l"));
        header.push_back(std::make_tuple("Tot-Drive-Hours", 0, "+l"));
        header.push_back(std::make_tuple("Cloud$-Replica", 0, "+l"));
        header.push_back(std::make_tuple("Cloud$-Erasure", 0, "+l"));
      }

      table.SetHeader(header);
      double volyears = totalcapacity * totalhours / 24.0 / 365.0;
      double tbyears = totaltbhours / 24.0 / 365.0;
      double tage = totalhours / (totaldrivecount ? totaldrivecount : 1000000);
      double cloudinstancecost = tbyears * 250.0; // assume 250 cloud$ per tb/year
      gjson["cost"]["vol:years"] = volyears;
      gjson["cost"]["tb:years"] = tbyears;
      gjson["cost"]["avg-drive-hours"] = tage;
      gjson["cost"]["tot-drive-hours"] = totalhours;
      gjson["cost"]["cloud-dollar-replica"] = cloudinstancecost / 2.0;
      gjson["cost"]["cloud-dollar-erasure"] = cloudinstancecost / 1.2;
      TableRow row;

      if (format_case == DevicesProto::LsProto::MONITORING) {
        row.emplace_back(tbyears, "f");
        row.emplace_back(tage, "l");
        row.emplace_back(totalhours, "l");
        row.emplace_back(cloudinstancecost / 2.0, "l");
        row.emplace_back(cloudinstancecost / 1.2, "l");
      } else {
        row.emplace_back(gOFS->MgmOfsInstanceName.c_str(), "s");
        row.emplace_back(tbyears, "+l");
        row.emplace_back(tage, "+l");
        row.emplace_back(totalhours, "+l");
        row.emplace_back(cloudinstancecost / 2.0, "+l", "$");
        row.emplace_back(cloudinstancecost / 1.2, "+l", "$");
      }

      body.push_back(row);
      table.AddRows(body);

      if (!WantsJsonOutput()) {
        std_out += table.GenerateTable();
      }
    }
  }

  if (WantsJsonOutput()) {
    std_out = SSTR(gjson).c_str();
  }

  reply.set_std_out(std_out);
  reply.set_retc(0);
}
EOSMGMNAMESPACE_END

