// ----------------------------------------------------------------------
// File: StackTrace.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/ASwitzerland                                  *
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

/**
 * @file   StackTrace.hh
 * 
 * @brief  Class providing readable stack traces using GDB
 * 
 */


#ifndef __EOSCOMMON__STACKTRACE__HH
#define __EOSCOMMON__STACKTRACE__HH


EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Static Class implementing comfortable readable stack traces
//! To use this static functions include this header file 
/*----------------------------------------------------------------------------*/
class StackTrace
{
public:
  // ---------------------------------------------------------------------------
  //! Create a readable back trace using gdb
  // ---------------------------------------------------------------------------
  static void GdbTrace(const char* executable, pid_t pid, const char* what)
  {
    fprintf(stderr,"#########################################################################\n");
    fprintf(stderr,"# stack trace exec=%s pid=%u what='%s'\n",executable,(unsigned int) pid, what);
    fprintf(stderr,"#########################################################################\n");
    XrdOucString systemline;
    // if shared libraries loaded by the exectuable have been changed GDB can run wild, therefore
    // we limit the virtual memory to 10GB and put a timeout of 2 minutes to produce a stack strace
    systemline = "ulimit -v 10000000000; eossh-timeout -t 120 -i 10 gdb --quiet "; 
    systemline += executable;
    systemline += " -p ";
    systemline += (int) pid;
    systemline += " <<< ";
    systemline += "\"";
    systemline += what;
    systemline += "\" 2> /dev/null";
    systemline += "| awk '{if ($2 == \"quit\") {on=0} else { if (on ==1) {print}; if ($1 == \"(gdb)\") {on=1;};} }' 2>&1 ";
    std::string gdbdump = eos::common::StringConversion::StringFromShellCmd(systemline.c_str());
    fprintf(stderr,"%s\n",gdbdump.c_str());
    if (!strcmp("thread apply all bt", what))
    {
      // we can extract the signal thread from all thread back traces
      GdbSignaledTrace(gdbdump);
    }
    return;
  }

  // ---------------------------------------------------------------------------
  //! Extract the thread stack trace creating responsible signal
  // ---------------------------------------------------------------------------
  static void GdbSignaledTrace(std::string &trace)
  {
    // analyze the trace until we find '<signal handler called>' and extract this trace
    std::vector<std::string> lines;
    eos::common::StringConversion::Tokenize(trace, lines, "\n");
    size_t thread_start=0;
    size_t thread_stop=0;
    size_t trace_start = 0;
    for (size_t i=0; i< lines.size(); i++)
    {
      if (lines[i].substr(0,6) == "Thread")
      {
	if (thread_start && trace_start) 
	{
	  thread_stop = i -1;
	  break;
	}
        thread_start = i;
      }

      if (lines[i].length() <2)
      {
        thread_stop = i;
        if (trace_start)
          break;
      }
      if (lines[i].find("<signal handler called>") != std::string::npos)
      {
        trace_start = i;
      }
    }

    if (!thread_stop)
      thread_stop = lines.size()-1;

    if ( (thread_start < trace_start) &&
         (trace_start < thread_stop ) )
    {
      fprintf(stderr,"#########################################################################\n");
      fprintf(stderr,"# -----------------------------------------------------------------------\n");
      fprintf(stderr,"# Responsible thread =>\n");
      fprintf(stderr,"# -----------------------------------------------------------------------\n");
      fprintf(stderr,"# %s\n", lines[thread_start].c_str());
      fprintf(stderr,"#########################################################################\n");
      for (size_t l=trace_start; l<=thread_stop; l++)
      {
        fprintf(stderr,"%s\n",lines[l].c_str());
      }
    }
    else
    {
      fprintf(stderr,"#########################################################################\n");
      fprintf(stderr,"# warning: failed to parse the thread responsible for signal [%u %u %u]n", (unsigned int)thread_start, (unsigned int)trace_start, (unsigned int)thread_stop);
      fprintf(stderr,"#########################################################################\n");
    }
  }
};

EOSCOMMONNAMESPACE_END

#endif
