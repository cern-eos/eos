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
#include <fcntl.h>
#include <unistd.h>
#include <set>
#include <regex.h>

double max(std::multiset<float>& s)
{
  double lmax=0;
  for (auto it : s) {
    if (it > lmax) {
      lmax = it;
    }
  }
  return lmax;
}

double avg(std::multiset<float>& s)
{
  double sum=0;
  if (!s.size())
    return 0;

  for (auto it : s) {
    sum += it;
  }
  return sum/s.size();
}

double nperc(std::multiset<float>& s)
{
  size_t n = s.size();
  size_t n_perc = (size_t)(n * 99.0 / 100.0);
  size_t i=0;
  double nperc=0;
  for (auto it : s) {
    i++;
    if ( i >= n_perc ) {
      nperc = it;
      break;
    }
  }
  return nperc;
}

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
	  if (map["sec.app"] == "deletion") {
	    continue;
	  }
	  bool found=false;
	  // classify write or read
	  if (std::stol(map["wb"]) > 0) {
	    sum_w += std::stol(map["wb"]);
	    double tt = std::stoul(map["cts"]) - std::stoul(map["ots"]) + (0.001 * std::stoul(map["ctms"])) - (0.001 * std::stoul(map["otms"]));
	    float rate = std::stol(map["wb"]) / tt / 1000000.0;
	    if (!silent) fprintf(stdout,"W %-16s t=%03.02f [s] r=%03.02f [MB/s] path=%64s\n", eos::common::StringConversion::GetReadableSizeString(sizestring, std::stoul(map["wb"]),""), tt, rate,map["path"].c_str());
	    w_t.insert(tt);
	    found=true;
	  }

	  if (std::stol(map["rb"]) > 0) {
	    sum_r += std::stol(map["rb"]);
	    double tt = std::stoul(map["cts"]) - std::stoul(map["ots"]) + (0.001 * std::stoul(map["ctms"])) - (0.001 * std::stoul(map["otms"]));
	    float rate = std::stol(map["rb"]) / tt / 1000000.0;
	    if (!silent) fprintf(stdout,"R %-16s t=%03.02f [s] r=%03.02f [MB/s] path=%64s\n", eos::common::StringConversion::GetReadableSizeString(sizestring, std::stoul(map["rb"]),""), tt, rate, map["path"].c_str());
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
      fprintf(stdout," N(r)= %lu VOL(r)= %s N(w)= %lu VOL(w)= %s\n",
	      r_t.size(),
	      eos::common::StringConversion::GetReadableSizeString(sizestring1, sum_r,"B"),
	      w_t.size(),
	      eos::common::StringConversion::GetReadableSizeString(sizestring2, sum_w,"B"));
      fprintf(stdout,"r:t avg: %.02f 99-perc: %0.02f -| max: %.02f \n", avg(r_t), nperc(r_t), max(r_t));
      fprintf(stdout,"w:t avg: %.02f 99-perc: %0.02f -| max: %.02f \n", avg(w_t), nperc(w_t), max(w_t));
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
          "'[eos] report [-n <nrecords>] [--regex <regex>] [-s]' <reportfile>\n");
  fprintf(stdout, "Usage: report <file>\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "          -s         : show only the summary with N(r) [number of files read] N(w) [number of files written] VOL(r) [data volume read] VOL (w) [data volume written],\n");
  fprintf(stdout, "                       + timings avg [average transfer time], 99-perc [99 percentila] max [maximal transfer time]\n");
  fprintf(stdout, "          -n <n>     : stop after n records are accepted for the statistics\n");
  fprintf(stdout, "     --regex <regex> : apply <regex> for filtering the records\n");
  global_retc = EINVAL;
  return (0);
}
