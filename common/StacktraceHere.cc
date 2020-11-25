// ----------------------------------------------------------------------
// File: StacktraceHere.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/ASwitzerland                                 *
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

#include "common/StacktraceHere.hh"

#ifndef __APPLE__
#include <execinfo.h>
#define BACKWARD_HAS_BFD 1
#if !defined(bfd_get_section_flags)
#define bfd_get_section_flags(ptr, section) bfd_section_flags(section)
#endif /* !defined(bfd_get_section_flags) */
#if !defined(bfd_get_section_size)
#define bfd_get_section_size(section) bfd_section_size(section)
#endif /* !defined(bfd_get_section_size) */
#if !defined(bfd_get_section_vma)
#define bfd_get_section_vma(ptr, section) bfd_section_vma(section)
#endif /* !defined(bfd_get_section_size) */
#include "common/backward-cpp/backward.hpp"
#endif

EOSCOMMONNAMESPACE_BEGIN

#ifdef __APPLE__
std::string getStacktrace()
{
  return "No stacktrack available on this platform";
}
#else
std::string getStacktrace()
{
  if (getenv("EOS_ENABLE_BACKWARD_STACKTRACE")) {
    // Very heavy-weight stacktrace, only use during development.
    std::ostringstream ss;
    backward::StackTrace st;
    st.load_here(128);
    backward::Printer p;
    p.object = true;
    p.address = true;
    p.print(st, ss);
    return ss.str();
  }

  std::ostringstream o;
  void* array[24];
  int size = backtrace(array, 24);
  char** messages = backtrace_symbols(array, size);

  // skip first stack frame (points here)
  for (int i = 1; i < size && messages != NULL; ++i) {
    char* mangled_name = 0, *offset_begin = 0, *offset_end = 0;

    // find parantheses and +address offset surrounding mangled name
    for (char* p = messages[i]; *p; ++p) {
      if (!p) {
        break;
      }

      if (*p == '(') {
        mangled_name = p;
      } else if (*p == '+') {
        offset_begin = p;
      } else if (*p == ')') {
        offset_end = p;
        break;
      }
    }

    // if the line could be processed, attempt to demangle the symbol
    if (mangled_name && offset_begin && offset_end &&
        mangled_name < offset_begin) {
      *mangled_name++ = '\0';
      *offset_begin++ = '\0';
      *offset_end++ = '\0';
      int status;
      char* real_name = abi::__cxa_demangle(mangled_name, 0, 0, &status);

      // if demangling is successful, output the demangled function name
      if (status == 0) {
        o << "[bt]: (" << i << ") " << messages[i] << " : "
          << real_name << "+" << offset_begin << offset_end
          << " " << std::endl;
      }
      // otherwise, output the mangled function name
      else {
        o << "[bt]: (" << i << ") " << messages[i] << " : "
          << mangled_name << "+" << offset_begin << offset_end
          << " " << std::endl;
      }

      free(real_name);
    }
    // otherwise, print the whole line
    else {
      o << "[bt]: (" << i << ") " << messages[i] << " " << std::endl;
    }
  }

  free(messages);
  return o.str();
}
#endif


#ifdef __APPLE__
void handleSignal(int sig, siginfo_t* si, void* ctx)
{
}
#else
void handleSignal(int sig, siginfo_t* si, void* ctx)
{
  if (!getenv("EOS_ENABLE_BACKWARD_STACKTRACE")) {
    return;
  }

  backward::SignalHandling::handleSignal(sig, si, ctx);
}
#endif

EOSCOMMONNAMESPACE_END
