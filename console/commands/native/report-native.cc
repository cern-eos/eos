// ----------------------------------------------------------------------
// File: report-native.cc
// ----------------------------------------------------------------------

#include "common/Path.hh"
#include "common/Statistics.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <fcntl.h>
#include <json/json.h>
#include <regex.h>
#include <set>
#include <sstream>
#include <unistd.h>

namespace {
class ReportCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "report";
  }
  const char*
  description() const override
  {
    return "Generate cluster report";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    eos::common::StringTokenizer subtokenizer(joined.c_str());
    subtokenizer.GetLine();
    XrdOucString arg;
    XrdOucString path;
    std::string sregex;
    size_t max_reports = 2000000000;
    bool silent = false;
    XrdOucString squash;
    time_t start_time = 0;
    time_t stop_time = 0;
    time_t first_ts = 0;
    time_t last_ts = 0;
    double max_eff = 100.0;
    bool reading = false;
    bool writing = false;
    bool json = false;
    Json::Value gjson;
    do {
      arg = subtokenizer.GetToken();
      if (!arg.length() && path.length())
        break;
      if (arg == "--regex") {
        arg = subtokenizer.GetToken();
        if (!arg.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        } else {
          sregex = arg.c_str();
        }
        continue;
      }
      if (arg == "-n") {
        arg = subtokenizer.GetToken();
        if (!arg.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        } else {
          max_reports = std::strtoul(arg.c_str(), 0, 10);
        }
        continue;
      }
      if (arg == "--read") {
        reading = true;
        continue;
      }
      if (arg == "--write") {
        writing = true;
        continue;
      }
      if (arg == "--json") {
        json = true;
        continue;
      }
      if (arg == "--max-efficiency") {
        arg = subtokenizer.GetToken();
        max_eff = strtod(arg.c_str(), 0);
        if ((max_eff < 0) || (max_eff > 100)) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        continue;
      }
      if (arg == "--squash") {
        arg = subtokenizer.GetToken();
        if (!arg.beginswith("/") && !arg.endswith("/")) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        squash = arg;
        continue;
      }
      if (arg == "--start") {
        arg = subtokenizer.GetToken();
        if (!arg.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        } else {
          start_time = std::strtoul(arg.c_str(), 0, 10);
        }
        continue;
      }
      if (arg == "--stop") {
        arg = subtokenizer.GetToken();
        if (!arg.length()) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        } else {
          stop_time = std::strtoul(arg.c_str(), 0, 10);
        }
        continue;
      }
      if (arg == "-s") {
        silent = true;
        continue;
      }
      if ((!arg.length()) || (arg.beginswith("--help")) ||
          (arg.beginswith("-h"))) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      path = arg;
    } while (arg.length());
    if (!reading && !writing) {
      reading = writing = true;
    }
    {
      std::string reportfile = path.c_str();
      std::ifstream file(reportfile);
      if (file.is_open()) {
        std::map<std::string, std::string> map;
        std::string line;
        std::vector<std::string> keys;
        std::multiset<float> r_t;
        std::multiset<float> w_t;
        uint64_t sum_w, sum_r;
        size_t n_w = 0;
        size_t n_r = 0;
        double reff = 0;
        double srleff = 0;
        double weff = 0;
        double swleff = 0;
        sum_w = sum_r = 0;
        std::string sizestring;
        size_t n_reports = 0;
        regex_t regex;
        if (sregex.length()) {
          int regexErrorCode = regcomp(&regex, sregex.c_str(), REG_EXTENDED);
          if (regexErrorCode) {
            fprintf(stderr,
                    "error: regular expression is invalid regex-rc=%d\n",
                    regexErrorCode);
            global_retc = EINVAL;
            return 0;
          }
        }
        while (std::getline(file, line)) {
          if (sregex.length()) {
            int result = regexec(&regex, line.c_str(), 0, NULL, 0);
            if (result == REG_NOMATCH) {
              continue;
            } else if (result != 0) {
              fprintf(stderr, "error: invalid regex\n");
              global_retc = EINVAL;
              return 0;
            }
          }
          std::map<std::string, std::string> map;
          if (eos::common::StringConversion::GetSpecialKeyValueMap(
                  line.c_str(), map, "=", "&", &keys)) {
            if (!sregex.length() && map["td"].substr(0, 6) == "daemon") {
              continue;
            }
            if (!map.count("rb") && !map.count("wb")) {
              continue;
            }
            if (map["sec.app"] == "deletion") {
              continue;
            }
            bool found = false;
            time_t start_ots = std::stoul(map["ots"]);
            time_t start_cts = std::stoul(map["cts"]);
            if (start_time) {
              if (start_ots < start_time) {
                continue;
              }
            }
            if (stop_time) {
              if (start_ots > stop_time) {
                continue;
              }
            }
            if (!first_ts) {
              first_ts = start_ots;
            }
            last_ts = start_cts;
            ssize_t wsize = std::stol(map["wb"]);
            ssize_t rsize = std::stol(map["rb"]);
            double iot = std::stod(map["iot"]);
            double idt = std::stod(map["idt"]);
            double lwt = std::stod(map["lwt"]);
            double lrt = std::stod(map["lrt"]);
            double lrvt = std::stod(map["lrvt"]);
            double deff = 100.0 - (iot ? (100.0 * idt / iot) : 0.0);
            double lreff = 100.0 * ((iot - lrt - lrvt) / iot);
            double lweff = 100.0 * ((iot - lwt) / iot);
            int eff = (int)(deff);
            if (deff > max_eff) {
              continue;
            }
            if (json && !silent) {
              Json::Value ljson;
              for (auto it = map.begin(); it != map.end(); ++it) {
                if (eos::common::StringConversion::IsDecimalNumber(
                        it->second)) {
                  ljson[it->first] =
                      (Json::Value::UInt64)strtoull(it->second.c_str(), 0, 10);
                } else if (eos::common::StringConversion::IsDouble(
                               it->second)) {
                  ljson[it->first] = strtod(it->second.c_str(), 0);
                } else {
                  ljson[it->first] = it->second;
                }
              }
              ljson["io"]["efficiency"]["total"] = deff;
              ljson["io"]["efficiency"]["disk"]["rd"] = lreff;
              ljson["io"]["efficiency"]["disk"]["wr"] = lweff;
              Json::StreamWriterBuilder builder;
              builder["indentation"] = "";
              std::string str = Json::writeString(builder, ljson);
              fprintf(stdout, "%s\n", str.c_str());
            }
            if (std::stol(map["wb"]) > 0 && writing) {
              sum_w += wsize;
              n_w++;
              weff += deff;
              swleff += lweff;
              double tt = std::stoul(map["cts"]) - std::stoul(map["ots"]) +
                          (0.001 * std::stoul(map["ctms"])) -
                          (0.001 * std::stoul(map["otms"]));
              float rate = wsize / tt / 1000000.0;
              if (!silent && !json) {
                fprintf(stdout,
                        "W %-16s t=%06.02f [s] r=%06.02f [MB/s] eff=%02d/%02d "
                        "[%%] path=%64s\n",
                        eos::common::StringConversion::GetReadableSizeString(
                            sizestring, wsize, ""),
                        tt, rate, eff, (int)lweff, map["path"].c_str());
              }
              w_t.insert(tt);
              found = true;
            }
            if (std::stol(map["rb"]) > 0 && reading) {
              sum_r += rsize;
              n_r++;
              reff += deff;
              srleff += lreff;
              double tt = std::stoul(map["cts"]) - std::stoul(map["ots"]) +
                          (0.001 * std::stoul(map["ctms"])) -
                          (0.001 * std::stoul(map["otms"]));
              float rate = rsize / tt / 1000000.0;
              if (!silent && !json && !squash.length()) {
                fprintf(stdout,
                        "R %-16s t=%06.02f [s] r=%06.02f [MB/s] eff=%02d/%02d "
                        "[%%] path=%64s\n",
                        eos::common::StringConversion::GetReadableSizeString(
                            sizestring, rsize, ""),
                        tt, rate, eff, (int)lreff, map["path"].c_str());
              }
              r_t.insert(tt);
              found = true;
            }
            if (found) {
              n_reports++;
            }
          } else {
            fprintf(stderr, "error: failed to parse '%s'\n", line.c_str());
          }
          if (n_reports >= max_reports) {
            break;
          }
          if (squash.length()) {
            std::string rpath = squash.c_str();
            rpath += map["path"].c_str();
            eos::common::Path cPath(rpath.c_str());
            fprintf(stderr, "info: squash %s\n", cPath.GetFullPath().c_str());
            cPath.MakeParentPath(0644);
            int fd = open(rpath.c_str(), O_CREAT | O_RDWR | O_APPEND,
                          S_IRWXU | S_IRWXG);
            if (fd >= 0) {
              (void)::write(fd, line.c_str(), line.length() + 1);
              (void)::close(fd);
            }
          }
        }
        if (!json) {
          std::string sizestring1, sizestring2;
          fprintf(stdout, "----------------------------------------------------"
                          "-----------------\n");
          fprintf(stdout, "- n(r): %lu vol(r): %s n(w): %lu vol(w): %s\n",
                  r_t.size(),
                  eos::common::StringConversion::GetReadableSizeString(
                      sizestring1, sum_r, "B"),
                  w_t.size(),
                  eos::common::StringConversion::GetReadableSizeString(
                      sizestring2, sum_w, "B"));
          fprintf(stdout, "----------------------------------------------------"
                          "-----------------\n");
          fprintf(stdout,
                  "- r:t avg: %s +- %s 95-perc: %s 99-perc: %s max: %s \n",
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::avg(r_t), 6, 2)
                      .c_str(),
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::sig(r_t), 6, 2)
                      .c_str(),
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::nperc(r_t, 95), 6, 2)
                      .c_str(),
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::nperc(r_t, 99), 6, 2)
                      .c_str(),
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::max(r_t), 6, 2)
                      .c_str());
          fprintf(stdout,
                  "- w:t avg: %s +- %s 95-perc: %s 99-perc: %s max: %s \n",
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::avg(w_t), 6, 2)
                      .c_str(),
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::sig(w_t), 6, 2)
                      .c_str(),
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::nperc(w_t, 95), 6, 2)
                      .c_str(),
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::nperc(w_t, 99), 6, 2)
                      .c_str(),
                  eos::common::StringConversion::GetFixedDouble(
                      eos::common::Statistics::max(w_t), 6, 2)
                      .c_str());
          fprintf(stdout, "----------------------------------------------------"
                          "-----------------\n");
          XrdOucString agestring;
          fprintf(stdout,
                  "- first-ts:%ld last-ts:%ld time-span:%ld s [ %s ] \n",
                  first_ts, last_ts, last_ts - first_ts,
                  eos::common::StringConversion::GetReadableAgeString(
                      agestring, last_ts - first_ts));
          fprintf(
              stdout, "- r:rate eff: %02d/%02d%% avg: %.02f MB/s\n",
              n_r ? ((int)(reff / n_r)) : 0, n_r ? ((int)(srleff / n_r)) : 0,
              (last_ts - first_ts) ? sum_r / 1000000.0 / (last_ts - first_ts)
                                   : 0);
          fprintf(
              stdout, "- w:rate eff: %02d/%02d%% avg: %.02f MB/s\n",
              n_w ? ((int)(weff / n_w)) : 0, n_w ? ((int)(swleff / n_w)) : 0,
              (last_ts - first_ts) ? sum_w / 1000000.0 / (last_ts - first_ts)
                                   : 0);
          fprintf(stdout, "----------------------------------------------------"
                          "-----------------\n");
        } else if (silent) {
          gjson["report"]["rd"]["n"] = (Json::Value::UInt64)n_r;
          gjson["report"]["timestamp"]["first"] = (Json::Value::UInt64)first_ts;
          gjson["report"]["timestamp"]["last"] = (Json::Value::UInt64)last_ts;
          gjson["report"]["wr"]["n"] = (Json::Value::UInt64)n_w;
          gjson["report"]["rd"]["bytes"]["sum"] = (Json::Value::UInt64)sum_r;
          gjson["report"]["wr"]["bytes"]["sum"] = (Json::Value::UInt64)sum_w;
          gjson["report"]["rd"]["bytes"]["avg"] =
              eos::common::Statistics::avg(r_t);
          gjson["report"]["wr"]["bytes"]["avg"] =
              eos::common::Statistics::avg(w_t);
          gjson["report"]["rd"]["bytes"]["sig"] =
              eos::common::Statistics::sig(r_t);
          gjson["report"]["wr"]["bytes"]["sig"] =
              eos::common::Statistics::sig(w_t);
          gjson["report"]["rd"]["bytes"]["max"] =
              eos::common::Statistics::max(r_t);
          gjson["report"]["wr"]["bytes"]["max"] =
              eos::common::Statistics::max(w_t);
          gjson["report"]["rd"]["bytes"]["95"] =
              eos::common::Statistics::nperc(r_t, 95);
          gjson["report"]["wr"]["bytes"]["95"] =
              eos::common::Statistics::nperc(w_t, 95);
          gjson["report"]["rd"]["bytes"]["99"] =
              eos::common::Statistics::nperc(r_t, 99);
          gjson["report"]["wr"]["bytes"]["99"] =
              eos::common::Statistics::nperc(w_t, 99);
          gjson["report"]["rd"]["rate"] =
              (last_ts - first_ts) ? sum_r / 1000000.0 / (last_ts - first_ts)
                                   : 0;
          gjson["report"]["wr"]["rate"] =
              (last_ts - first_ts) ? sum_w / 1000000.0 / (last_ts - first_ts)
                                   : 0;
          gjson["report"]["rd"]["efficiency"]["client"] =
              n_r ? (reff / n_r) : 0;
          gjson["report"]["rd"]["efficiency"]["server"] =
              n_r ? (srleff / n_r) : 0;
          gjson["report"]["rd"]["efficiency"]["client"] =
              n_w ? (weff / n_w) : 0;
          gjson["report"]["rd"]["efficiency"]["server"] =
              n_w ? (swleff / n_w) : 0;
          Json::StreamWriterBuilder builder2;
          builder2["indentation"] = "";
          std::string out = Json::writeString(builder2, gjson);
          fprintf(stdout, "%s", out.c_str());
        }
        file.close();
        if (sregex.length())
          regfree(&regex);
      } else {
        fprintf(stderr, "error: unable to open file!\n");
        global_retc = EIO;
        return 0;
      }
    }
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr,
            "Usage: report [--read|--write] [--regex <pattern>] [-n <max>]\n");
    fprintf(stderr,
            "               [--max-efficiency <0-100>] [--squash <path>]\n");
    fprintf(stderr,
            "               [--start <epoch>] [--stop <epoch>] [--json] [-s] <file>\n");
    fprintf(stderr,
            "options:\n");
    fprintf(stderr, "  --read                 filter read reports only\n");
    fprintf(stderr, "  --write                filter write reports only\n");
    fprintf(stderr, "  --regex <pattern>      POSIX regex to filter lines\n");
    fprintf(stderr, "  -n <max>               limit number of reports (default huge)\n");
    fprintf(stderr,
            "  --max-efficiency <n>   reject entries with efficiency above n (0-100)\n");
    fprintf(stderr,
            "  --squash <path>        squash paths to the given prefix (must start/end with '/')\n");
    fprintf(stderr, "  --start <epoch>        only include reports >= start time\n");
    fprintf(stderr, "  --stop <epoch>         only include reports <= stop time\n");
    fprintf(stderr, "  --json                 output JSON summary\n");
    fprintf(stderr, "  -s                     silent (no output)\n");
  }
};
} // namespace

void
RegisterReportNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ReportCommand>());
}
