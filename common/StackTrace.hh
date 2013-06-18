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
    systemline = "gdb --quiet "; 
    systemline += executable;
    systemline += " -p ";
    systemline += (int) pid;
    systemline += " <<< ";
    systemline += "\"";
    systemline += what;
    systemline += "\" 2> /dev/null";
    systemline += "| awk '{if ($2 == \"quit\") {on=0} else { if (on ==1) {print}; if ($1 == \"(gdb)\") {on=1;};} }' 1>&2 ";
    system(systemline.c_str());
  }
};

EOSCOMMONNAMESPACE_END

#endif
