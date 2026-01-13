// ----------------------------------------------------------------------
// File: Shutdown.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

//------------------------------------------------------------------------------
/*
 * @brief shutdown function cleaning up running threads/objects for a clean exit
 *
 * @param sig signal catched
 *
 * This shutdown function tries to get a write lock before doing the namespace
 * shutdown. Since it is not guaranteed that one can always get a write lock
 * there is a timeout in requiring the write lock and then the shutdown is forced.
 * Depending on the role of the MGM it stop's the running namespace follower
 * and in all cases running sub-services of the MGM.
 */
//------------------------------------------------------------------------------
void
xrdmgmofs_shutdown(int sig)

{
  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  eos_static_alert("msg=\"shutdown sequence started\'");

  // Avoid shutdown recursions
  if (gOFS->Shutdown) {
    return;
  }

  gOFS->Shutdown = true;
  gOFS->OrderlyShutdown();
  eos_static_alert("%s", "msg=\"shutdown complete\"");
  std::quick_exit(0);
}
