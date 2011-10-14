/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Report.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

Report::Report(XrdOucEnv &report) 
{
  ots = report.Get("ots")?strtoull(report.Get("ots"),0,10):0;
  cts = report.Get("cts")?strtoull(report.Get("cts"),0,10):0;
  otms = report.Get("otms")?strtoull(report.Get("otms"),0,10):0;
  ctms = report.Get("ctms")?strtoull(report.Get("ctms"),0,10):0;
  logid = report.Get("log")?report.Get("log"):"";
  path  = report.Get("path")?report.Get("path"):"";
  uid   = (uid_t) atoi(report.Get("ruid")?report.Get("ruid"):"0");
  gid   = (gid_t) atoi(report.Get("rgid")?report.Get("rgid"):"0");
  td    = report.Get("td")?report.Get("td"):"none";
  host  = report.Get("host")?report.Get("host"):"none";
  lid   = strtoul(report.Get("lid")?report.Get("lid"):"0",0,10);
  fid   = strtoull(report.Get("fid")?report.Get("fid"):"0",0,10);
  fsid  = strtoul(report.Get("fsid")?report.Get("fsid"):"0",0,10);
  rb    = strtoull(report.Get("rb")?report.Get("rb"):"0",0,10);
  wb    = strtoull(report.Get("wb")?report.Get("wb"):"0",0,10);
  srb    = strtoull(report.Get("srb")?report.Get("srb"):"0",0,10);
  swb    = strtoull(report.Get("swb")?report.Get("swb"):"0",0,10);
  nrc    = strtoull(report.Get("nrc")?report.Get("nrc"):"0",0,10);
  nwc    = strtoull(report.Get("nwc")?report.Get("nwc"):"0",0,10);
  rt     = atof(report.Get("rt")?report.Get("rt"):"0.0");
  wt     = atof(report.Get("wt")?report.Get("wt"):"0.0");
  osize  = strtoull(report.Get("osize")?report.Get("osize"):"0",0,10);
  csize  = strtoull(report.Get("csize")?report.Get("csize"):"0",0,10);
}

void
Report::Dump(XrdOucString &out)
{
  char dumpline[16384];
  snprintf(dumpline,sizeof(dumpline)-1,"uid=%d gid=%d rb=%llu wb=%llu srb=%llu swb=%llu nrc=%llu nwc=%llu rt=%.02f wt=%.02f osize=%llu csize=%llu ots=%llu.%llu cts=%llu.%llu td=%s host=%s logid=%s\n", uid, gid, rb, wb, srb, swb, nrc,nwc, rt,wt,osize,csize, ots,otms, cts,ctms, td.c_str(),host.c_str(), logid.c_str());
  out+=dumpline;
}

EOSCOMMONNAMESPACE_END

