// ----------------------------------------------------------------------
// File: com_console.cc
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
#include "console/ConsoleMain.hh"
#include <XrdSys/XrdSysTimer.hh>
#include <XrdSys/XrdSysLogger.hh>
#include "mq/ErrorReportListener.hh"
/*----------------------------------------------------------------------------*/

#include <sys/types.h>
#include <sys/wait.h>


#ifdef CLIENT_ONLY

int
com_console(char* arg)
{
  fprintf(stderr, "error: console not supported in client-only compilation\n");
  exit(-1);
}
#else

/* Run error log console &*/
int
com_console(char* arg)
{
  XrdSysLogger* logger = 0;
  XrdOucString sarg = arg;

  if (arg) {
    if (sarg.beginswith("\"log\"")) {
      logger = new XrdSysLogger();
      XrdSysError eDest(logger);
      XrdOucString loggerpath = "/var/log/eos/mgm/";
      loggerpath += "error.log";
      logger->Bind(loggerpath.c_str(), 0);
    } else {
      if (sarg != "") {
        fprintf(stderr, "usage: console [log]\n");
        fprintf(stderr,
                "                                 log - write a log file into /var/log/eos/mgm/error.log\n");
        exit(-1);
      }
    }
  }

  char c_hostname[256] = {0};

  if (gethostname(c_hostname, sizeof(c_hostname)) || !strlen(c_hostname)) {
    fprintf(stderr, "error: failed to retrieve hostname errno=%d", errno);
    exit(-1);
  }

  eos::mq::ErrorReportListener listener(serveruri.c_str(), c_hostname);

  while (1) {
    std::string newmessage;
    listener.fetch(newmessage);

    if (!newmessage.empty()) {
      XrdOucString line = newmessage.c_str();

      if (global_highlighting) {
        static std::string textnormal("\033[0m");
        static std::string textblack("\033[49;30m");
        static std::string textred("\033[49;31m");
        static std::string textrederror("\033[47;31m\e[5m");
        static std::string textblueerror("\033[47;34m\e[5m");
        static std::string textgreen("\033[49;32m");
        static std::string textyellow("\033[49;33m");
        static std::string textblue("\033[49;34m");
        static std::string textbold("\033[1m");
        static std::string textunbold("\033[0m");
        static std::string cinfo = textgreen + "INFO" + textnormal;
        static std::string cdebug = textblack + "DEBUG" + textnormal;
        static std::string cerr = textred + "ERROR" + textnormal;
        static std::string cnote = textblue + "NOTE" + textnormal;
        static std::string cwarn = textblueerror + "WARN" + textnormal;
        static std::string cemerg = textrederror + "EMERG" + textnormal;
        static std::string ccrit = textrederror + "CRIT" + textnormal;
        static std::string calert = textrederror + "ALERT" + textnormal;
        line.replace("INFO", cinfo.c_str());
        line.replace("DEBUG", cdebug.c_str());
        line.replace("ERROR", cerr.c_str());
        line.replace("EMERG", cemerg.c_str());
        line.replace("CRIT", ccrit.c_str());
        line.replace("WARN", cwarn.c_str());
        line.replace("ALERT", calert.c_str());
        line.replace("NOTE", cnote.c_str());
      }

      if (logger) {
        fprintf(stderr, "%s\n", line.c_str());
      } else {
        fprintf(stdout, "%s\n", line.c_str());
        fflush(stdout);
      }
    } else {
      XrdSysTimer sleeper;
      sleeper.Wait(100);
    }
  }

  return (0);
}
#endif
