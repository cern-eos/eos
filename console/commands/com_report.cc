// ----------------------------------------------------------------------
// File: com_report.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "common/Statistics.hh"
#include <fcntl.h>
#include <unistd.h>
#include <set>
#include <regex.h>

/* Change working directory &*/
int
com_report(char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();

  XrdOucString arg;
  XrdOucString path;

  std::string sregex;
  size_t max_reports=2000000000;
  bool silent = false;
  bool ec = false;
  time_t start_time=0;
  time_t stop_time=0;

  do {
    arg = subtokenizer.GetToken();
    if (!arg.length() && path.length()) {
      break;
    }
    if ( arg == "--regex" ) {
      arg = subtokenizer.GetToken();
      if (!arg.length()) {
	goto com_report_usage;
      } else {
	sregex = arg.c_str();
      }
      continue;
    }

    if ( arg == "-n") {
      arg = subtokenizer.GetToken();
      if (!arg.length()) {
	goto com_report_usage;
      } else {
	max_reports = std::strtoul(arg.c_str(),0,10);
      }
      continue;
    }

    if ( arg == "--ec") {
      ec = true;
      continue;
    }

    if ( arg == "--start") {
      arg = subtokenizer.GetToken();
      if (!arg.length()) {
	goto com_report_usage;
      } else {
	start_time = std::strtoul(arg.c_str(),0,10);
      }
      continue;
    }

    if ( arg == "--stop") {
      arg = subtokenizer.GetToken();
      if (!arg.length()) {
	goto com_report_usage;
      } else {
	stop_time = std::strtoul(arg.c_str(),0,10);
      }
      continue;
    }

    if ( arg == "-s" ) {
      silent = true;
      continue;
    }
    if ((!arg.length()) || (arg.beginswith("--help")) || (arg.beginswith("-h"))) {
      goto com_report_usage;
    }

    path = arg;
  } while (arg.length());


  {
    std::string reportfile = path.c_str();

    std::ifstream file(reportfile);
    if (file.is_open()) {
      std::map<std::string,std::string> map;
      std::string line;
      std::vector<std::string> keys;
      std::multiset<float> r_t;
      std::multiset<float> w_t;
      uint64_t sum_w,sum_r;
      sum_w = sum_r = 0;
      std::string sizestring;
      size_t n_reports=0;
      regex_t regex;
      if (sregex.length()) {
	// Compile regex
	int regexErrorCode = regcomp(&regex, sregex.c_str(), REG_EXTENDED);

	if (regexErrorCode) {
	  fprintf(stderr,"error: regular expression is invalid regex-rc=%d\n", regexErrorCode);
	  global_retc = EINVAL;
	return (0);
	}
      }

      while (std::getline(file, line)) {

	if (sregex.length()) {
	  // Execute regex
	  int result = regexec(&regex, line.c_str(), 0, NULL, 0);
	  // Check the result
	  if (result == REG_NOMATCH) {
	    // next entry
	    continue;
	  } else if (result != 0) { // REG_BADPAT, REG_ESPACE, etc...
	    fprintf(stderr,"error: invalid regex\n");
	    global_retc = EINVAL;
	    return (0);
	  } else {
	  }
	}

	if (eos::common::StringConversion::GetKeyValueMap(line.c_str(),
							map,
							  "=",
							  "&",
							  &keys)) {

	  if (!sregex.length() && map["td"].substr(0,6) == "daemon") {
	    continue;
	  }

	  if (!map.count("rb") && !map.count("wb")) {
	    continue;
	  }

	  if (map["sec.app"] == "deletion") {
	    continue;
	  }
	  bool found=false;

	  time_t start_ots = std::stoul(map["ots"]);

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
	  ssize_t wsize = ec?std::stol(map["csize"]):std::stol(map["wb"]);
	  ssize_t rsize = ec?std::stol(map["csize"]):std::stol(map["rb"]);
	  // classify write or read
	  if (std::stol(map["wb"]) > 0) {
	    sum_w += wsize;
	    double tt = std::stoul(map["cts"]) - std::stoul(map["ots"]) + (0.001 * std::stoul(map["ctms"])) - (0.001 * std::stoul(map["otms"]));
	    float rate = wsize  / tt / 1000000.0;
	    if (!silent) fprintf(stdout,"W %-16s t=%03.02f [s] r=%03.02f [MB/s] path=%64s\n", eos::common::StringConversion::GetReadableSizeString(sizestring, wsize,""), tt, rate,map["path"].c_str());
	    w_t.insert(tt);
	    found=true;
	  }
	  if (std::stol(map["rb"]) > 0) {
	    sum_r += rsize;
	    double tt = std::stoul(map["cts"]) - std::stoul(map["ots"]) + (0.001 * std::stoul(map["ctms"])) - (0.001 * std::stoul(map["otms"]));
	    float rate = rsize / tt / 1000000.0;
	    if (!silent) fprintf(stdout,"R %-16s t=%03.02f [s] r=%03.02f [MB/s] path=%64s\n", eos::common::StringConversion::GetReadableSizeString(sizestring, rsize,""), tt, rate, map["path"].c_str());
	    r_t.insert(tt);
	    found=true;
	  }
	  if (found) {
	    n_reports++;
	  }
	} else {
	  fprintf(stderr,"error: failed to parse '%s'\n", line.c_str());
	}
	if (n_reports >= max_reports) {
	  break;
	}
      }
      std::string sizestring1, sizestring2;

      fprintf(stdout,"---------------------------------------------------------------\n");
      fprintf(stdout,"n(r): %lu vol(r): %s n(w): %lu vol(w): %s\n",
	      r_t.size(),
	      eos::common::StringConversion::GetReadableSizeString(sizestring1, sum_r,"B"),
	      w_t.size(),
	      eos::common::StringConversion::GetReadableSizeString(sizestring2, sum_w,"B"));
      fprintf(stdout,"---------------------------------------------------------------\n");
      fprintf(stdout,"r:t avg: %s +- %s 95-perc: %s 99-perc: %s max: %s \n",
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::avg(r_t),6,2).c_str(),
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::sig(r_t),6,2).c_str(),
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::nperc(r_t,95),6,2).c_str(),
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::nperc(r_t,99),6,2).c_str(),
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::max(r_t),6,2).c_str()
	      );
      fprintf(stdout,"w:t avg: %s +- %s 95-perc: %s 99-perc: %s max: %s \n",
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::avg(w_t),6,2).c_str(),
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::sig(w_t),6,2).c_str(),
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::nperc(w_t,95),6,2).c_str(),
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::nperc(w_t,99),6,2).c_str(),
	      eos::common::StringConversion::GetFixedDouble(eos::common::Statistics::max(w_t),6,2).c_str()
	      );
      fprintf(stdout,"---------------------------------------------------------------\n");

      file.close();
      if (sregex.length()) {
	regfree(&regex);
      }
    } else {
      fprintf(stderr,"error: unable to open file!\n");
      global_retc = EIO;
      return (0);
    }
  }

  return (0);
com_report_usage:
  fprintf(stdout,
          "'[eos] report [-n <nrecords>] [--regex <regex>] [-s] [--start <unixtime>] [--stop <unixtime>] [--ec] <reportfile>\n");
  fprintf(stdout, "Usage: report <file>\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "          -s         : show only the summary with N(r) [number of files read] N(w) [number of files written] VOL(r) [data volume read] VOL (w) [data volume written],\n");
  fprintf(stdout, "                       + timings avg [average transfer time], 95-perc [95 percentile], 99-perc [99 percentila] max [maximal transfer time]\n");
  fprintf(stdout, "          -n <n>     : stop after n records are accepted for the statistics\n");
  fprintf(stdout,"           --ec       : consider records as EC file streaming reads/writes\n");
  fprintf(stdout, "     --regex <regex> : apply <regex> for filtering the records\n");
  fprintf(stdout, "  --start <unixtime> : only take records starting after <unixtime>\n");
  fprintf(stdout, "  --stop <unixtime>  : only take records starting before <unixtime>\n\n");
  fprintf(stdout, "Example:               bash> eos report /var/eos/report/2021/05/20210530.eosreport\n");
  fprintf(stdout, "                       bash> zcat /var/eos/report/2021/05/20210530.eosreport.gz | eos report /dev/stdin -s\n");
  fprintf(stdout, "                       bash> eos report /var/eos/report/2021/05/20210530.eosreport --regex \"sec.app=fuse\" -s\n");
  global_retc = EINVAL;
  return (0);
}
