// ----------------------------------------------------------------------
// File: JeMallocHandler.hh
// Author: Geoffray Adde - CERN
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

#ifndef __EOSCOMMON_JEMALLOCHANDLER__HH__
#define __EOSCOMMON_JEMALLOCHANDLER__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include <cstddef>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/*                                                                            */
/*----------------------------------------------------------------------------*/

class JeMallocHandler
{
  bool pJeMallocLoaded;
  bool pCanProfile;
  bool pProfRunning;
  int  (*mallctl)(const char *, void *, size_t *, void *, size_t );

  bool IsJemallocLoader();

  bool IsProfEnabled();

  bool IsProfgRunning();

public:
  JeMallocHandler();
  ~JeMallocHandler();
  inline bool JeMallocLoaded() { return pJeMallocLoaded; }
  inline bool CanProfile() { return pCanProfile; }
  inline bool ProfRunning() { return IsProfgRunning(); }
  bool StartProfiling();
  bool StopProfiling();
  bool DumpProfile();
};

EOSCOMMONNAMESPACE_END

#endif	/* __EOSCOMMON_JEMALLOCHANDLER__HH__ */

