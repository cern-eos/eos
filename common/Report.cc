// ----------------------------------------------------------------------
// File: Report.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Report.hh"

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/** 
 * Create a Report object based on a report env representation
 * 
 * @param report 
 */
/*----------------------------------------------------------------------------*/
Report::Report (XrdOucEnv &report)
{
  ots = report.Get("ots") ? strtoull(report.Get("ots"), 0, 10) : 0;
  cts = report.Get("cts") ? strtoull(report.Get("cts"), 0, 10) : 0;
  otms = report.Get("otms") ? strtoull(report.Get("otms"), 0, 10) : 0;
  ctms = report.Get("ctms") ? strtoull(report.Get("ctms"), 0, 10) : 0;
  logid = report.Get("log") ? report.Get("log") : "";
  path = report.Get("path") ? report.Get("path") : "";
  uid = (uid_t) atoi(report.Get("ruid") ? report.Get("ruid") : "0");
  gid = (gid_t) atoi(report.Get("rgid") ? report.Get("rgid") : "0");
  td = report.Get("td") ? report.Get("td") : "none";
  host = report.Get("host") ? report.Get("host") : "none";
  server_name = host;
  server_domain = host;
  int dpos = host.find(".");
  if (dpos != STR_NPOS)
  {
    server_name.erase(dpos);
    server_domain.erase(0, dpos + 1);
  }
  lid = strtoul(report.Get("lid") ? report.Get("lid") : "0", 0, 10);
  fid = strtoull(report.Get("fid") ? report.Get("fid") : "0", 0, 10);
  fsid = strtoul(report.Get("fsid") ? report.Get("fsid") : "0", 0, 10);
  rb = strtoull(report.Get("rb") ? report.Get("rb") : "0", 0, 10);
  rb_min = strtoull(report.Get("rb_min") ? report.Get("rb_min") : "0", 0, 10);
  rb_max = strtoull(report.Get("rb_max") ? report.Get("rb_max") : "0", 0, 10);
  rb_sigma = strtoull(report.Get("rb_sigma") ? report.Get("rb_sigma") : "0", 0, 10);
  wb = strtoull(report.Get("wb") ? report.Get("wb") : "0", 0, 10);
  wb_min = strtoull(report.Get("wb_min") ? report.Get("wb_min") : "0", 0, 10);
  wb_max = strtoull(report.Get("wb_max") ? report.Get("wb_max") : "0", 0, 10);
  wb_sigma = strtod(report.Get("wb_sigma") ? report.Get("wb_sigma") : "0", 0);
  sfwdb = strtoull(report.Get("sfwdb") ? report.Get("sfwdb") : "0", 0, 10);
  sbwdb = strtoull(report.Get("sbwdb") ? report.Get("sbwdb") : "0", 0, 10);
  sxlfwdb = strtoull(report.Get("sxlfwd") ? report.Get("sxlfwd") : "0", 0, 10);
  sxlbwdb = strtoull(report.Get("sxlbwd") ? report.Get("sxlbwd") : "0", 0, 10);
  nrc = strtoull(report.Get("nrc") ? report.Get("nrc") : "0", 0, 10);
  nwc = strtoull(report.Get("nwc") ? report.Get("nwc") : "0", 0, 10);
  nfwds = strtoull(report.Get("nfwds") ? report.Get("nfwds") : "0", 0, 10);
  nbwds = strtoull(report.Get("nbwds") ? report.Get("nbwds") : "0", 0, 10);
  nxlfwds = strtoull(report.Get("nxlfwds") ? report.Get("nxlfwds") : "0", 0, 10);
  nxlbwds = strtoull(report.Get("nxlbwds") ? report.Get("nxlbwds") : "0", 0, 10);
  rt = atof(report.Get("rt") ? report.Get("rt") : "0.0");
  wt = atof(report.Get("wt") ? report.Get("wt") : "0.0");
  osize = strtoull(report.Get("osize") ? report.Get("osize") : "0", 0, 10);
  csize = strtoull(report.Get("csize") ? report.Get("csize") : "0", 0, 10);
  // sec extensions
  sec_prot = report.Get("sec.prot") ? report.Get("sec.prot") : "";
  sec_name = report.Get("sec.name") ? report.Get("sec.name") : "";
  sec_host = report.Get("sec.host") ? report.Get("sec.host") : "";
  sec_domain = report.Get("sec.host") ? report.Get("sec.host") : "";
  dpos = sec_host.find(".");
  if (dpos != STR_NPOS)
  {
    sec_host.erase(dpos);
    sec_domain.erase(0, dpos + 1);
  }
  sec_vorg = report.Get("sec.vorg") ? report.Get("sec.vorg") : "";
  sec_role = report.Get("sec.role") ? report.Get("sec.role") : "";
  sec_info = report.Get("sec.info") ? report.Get("sec.info") : "";
  sec_app = report.Get("sec.app") ? report.Get("sec.app") : "";
  if (sec_app.find("?") != std::string::npos)
  {
    sec_app.erase(sec_app.find("?"));
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Dump the report contents into a string in human readable key=value format
 * 
 * @param out string containing the report
 */

/*----------------------------------------------------------------------------*/
void
Report::Dump (XrdOucString &out, bool dumpsec)
{
  char dumpline[16384];
  snprintf(dumpline, sizeof (dumpline) - 1, "uid=%d gid=%d rb=%llu rb_min=%llu rb_max=%llu rb_sigma=%.02f wb=%llu wb_min=%llu wb_max=%llu wb_sigma=%.02f sfwdb=%llu sbwdb=%llu sxlfwdb=%llu sxlbwdb=%llu nrc=%llu nwc=%llu nfwds=%llu nbwds=%llu nxlfwds=%llu nxlbwds=%llu rt=%.02f wt=%.02f osize=%llu csize=%llu ots=%llu.%llu cts=%llu.%llu td=%s host=%s logid=%s", uid, gid, rb, rb_min, rb_max, rb_sigma, wb, wb_min, wb_max, wb_sigma, sfwdb, sbwdb, sxlfwdb, sxlbwdb, nrc, nwc, nfwds, nbwds, nxlfwds, nxlbwds, rt, wt, osize, csize, ots, otms, cts, ctms, td.c_str(), host.c_str(), logid.c_str());
  out += dumpline;
  if (dumpsec)
  {
    snprintf(dumpline, sizeof (dumpline) - 1, " sec_prot=\"%s\" sec_name=\"%s\" sec_host=\"%s\" sec_vorg=\"%s\" sec_grps=\"%s\" sec_role=\"%s\" sec_info=\"%s\" sec_app=\"%s\"", sec_prot.c_str(), sec_name.c_str(), sec_host.c_str(), sec_vorg.c_str(), sec_grps.c_str(), sec_role.c_str(), sec_info.c_str(), sec_app.c_str());
    out += dumpline;
  }
  out += "\n";
}
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END

