// ----------------------------------------------------------------------
// File: Coverage.cc
// Author: Mihai Patrascoiu - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
/* @brief profiling function flushing coverage data
 *
 * @param sig signal caught
 *
 * Prints the collected gcov data upon receiving the signal.
 * The data should be collected via a tool capable of processing gcov output.
 */
/*----------------------------------------------------------------------------*/
void
xrdmgmofs_coverage(int sig)
{
#ifdef COVERAGE_BUILD
  eos_static_notice("msg=\"printing coverage data\"");
  __gcov_dump();

  // Get a map of all the loaded dynamic libraries
  using eos::common::PluginManager;
  PluginManager& pm = PluginManager::GetInstance();
  PluginManager::DynamicLibMap dynamicLibMap = pm.GetDynamicLibMap();

  typedef void (*CoverageFunc)();

  // Call the exported coverage function on each dynamic library
  for (auto& dLib: dynamicLibMap) {
    CoverageFunc coverageFunc =
        (CoverageFunc) dLib.second->GetSymbol("plugin_coverage");

    if (coverageFunc != NULL) {
      eos_static_notice("msg=\"calling exported coverage function for: %s\"",
                        dLib.first.c_str());
      coverageFunc();
    }
  }

  return;
#endif

  eos_static_notice("msg=\"compiled without coverage support\"");
}
