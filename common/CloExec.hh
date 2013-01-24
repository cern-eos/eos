// ----------------------------------------------------------------------
// File: CloExec.hh
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
 * @file   CloExec.hh
 * 
 * @brief  Class setting filedescriptor flags to CLOEXEC
 *  
 */

#ifndef __EOSCOMMON__CLOEXEC__HH
#define __EOSCOMMON__CLOEXEC__HH

#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "common/Namespace.hh"
#include <fcntl.h>
#include <sys/time.h>

EOSCOMMONNAMESPACE_BEGIN;

/*----------------------------------------------------------------------------*/
//! @brief Static Class to set a filedescriptor flag to CLOEXEC
//! 
//! Example
//! \code eos::common::CloExec::Set(fd); \endcode
/*----------------------------------------------------------------------------*/
class CloExec {
public:

    // ---------------------------------------------------------------------------
    //! Set CLOEXEC on a single fd. 
    // ---------------------------------------------------------------------------

    static int Set(int fd) {
        int flags = 0;
        if ((flags = fcntl(fd, F_GETFD)) != -1) {
            return fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
        } else {
            return -1;
        }
    }

    // ---------------------------------------------------------------------------
    //! Set CLOEXEC on all used file descriptors.
    // ---------------------------------------------------------------------------

    static void All() {
        for (size_t i = getdtablesize(); i-- > 3;) {
            eos::common::CloExec::Set(i);
        }
    }
};

EOSCOMMONNAMESPACE_END;

#endif
