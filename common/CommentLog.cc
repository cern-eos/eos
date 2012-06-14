// ----------------------------------------------------------------------
// File: CommentLog.cc
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
#include "common/CommentLog.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
/** 
 * Open/Create the comment log file
 * 
 * @param file path to the comment log file
 */
/*----------------------------------------------------------------------------*/
CommentLog::CommentLog(const char* file)
{
  fName = file;
  fFd = open(file,O_CREAT| O_RDWR, 0644 );
}

/*----------------------------------------------------------------------------*/
/** 
 * Destructor
 * 
 */
/*----------------------------------------------------------------------------*/
CommentLog::~CommentLog() 
{
  if (fFd>0) {
    close(fFd);
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Check if the comment log file is valid (opened)
 * 
 * 
 * @return true if valid - false if error
 */
/*----------------------------------------------------------------------------*/
bool 
CommentLog::IsValid() 
{
  return (fFd>0);
}

/*----------------------------------------------------------------------------*/
/** 
 * Add a comment log line to the log file
 * 
 * @param t timestamp to be displayed
 * @param cmd command to be displayed
 * @param subcmd subcommand to be displayed
 * @param args used for cmd/subcmd to be displayed
 * @param comment to be displayed
 * @param stdErr error returned to the shell for this command to be displayed
 * @param retc return code returned to the shell for this command to be displayed
 * 
 * @return true if successful - false if error
 */
/*----------------------------------------------------------------------------*/
bool 
CommentLog::Add(time_t t, const char* cmd, const char* subcmd, const char* args, const char* comment, const char* stdErr, int retc)
{
  XrdOucString out="";
  struct tm * timeinfo;
  timeinfo = localtime (&t);

  out += "# ==============================================================\n";
  out += "# "; out += asctime(timeinfo); out.erase(out.length()-1); out += " "; out += comment; out += "\n";
  out += "# --------------------------------------------------------------\n";
  char st[16];
  snprintf(st, sizeof(st)-1,"%u", (unsigned int) t);
  out += "  time="; out += st; out += " cmd=\""; out += cmd; out += "\" subcmd=\""; out += subcmd; out += "\" retc="; out += retc; out += " comment=", out += comment; out += "\n";
  out += "# ..............................................................\n";
  out += "# args: "; out += args; out += "\n";
  XrdOucString sErr = stdErr;
  if (sErr.length()) {
    while (sErr.replace("\n","__#n#__")) {}
    while (sErr.replace("__#n#__","\n# ")) {}
    sErr.insert("# ",0);
    if (sErr.endswith("#")) {
      sErr.erase(sErr.length()-1);
    }
    out += "# >STDERR\n";
    out += sErr;
  }
  if (!out.endswith("\n")) {
    out += "\n";
  }
  if ( (write(fFd, out.c_str(), out.length() +1)) < 0) {
    return false;
  } else {
    return true;
  }
}


/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
