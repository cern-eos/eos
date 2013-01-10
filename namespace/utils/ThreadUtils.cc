//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Threading utilities
//------------------------------------------------------------------------------
// EOS - the CERN Disk Storage System
// Copyright (C) 2013 CERN/Switzerland
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

#include "namespace/utils/ThreadUtils.hh"
#include <signal.h>

namespace eos
{
  //----------------------------------------------------------------------------
  // Block the signals that XRootD uses to handle asynchronous IO
  //----------------------------------------------------------------------------
  void ThreadUtils::blockAIOSignals()
  {
#if !defined(__APPLE__) && defined(_POSIX_ASYNCHRONOUS_IO)
#ifdef SIGRTMAX
    const int OSS_AIO_READ_DONE  = SIGRTMAX-1;
    const int OSS_AIO_WRITE_DONE = SIGRTMAX;
#else
    const int OSS_AIO_READ_DONE  = SIGUSR1;
    const int OSS_AIO_WRITE_DONE = SIGUSR2;
#endif
    sigset_t signalMask;
    sigemptyset( &signalMask );
    sigaddset( &signalMask, OSS_AIO_READ_DONE );
    sigaddset( &signalMask, OSS_AIO_WRITE_DONE );
    pthread_sigmask( SIG_BLOCK, &signalMask, NULL );
#endif
  }
}
