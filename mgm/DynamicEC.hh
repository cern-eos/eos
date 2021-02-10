// ----------------------------------------------------------------------
// File: DynamicEC.hh
// Author: Andreas Stoeve - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @file DynamicEC.hh
//! @breif removing of old partision wich is not in use
//------------------------------------------------------------------------------



#ifndef __EOSMGM_GEOBALANCER__
#define __EOSMGM_GEOBALANCER__

/* -------------------------------------------------------------------------- */
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/AssistedThread.hh"
/* -------------------------------------------------------------------------- */
#include "XrdSys/XrdSysPthread.hh"
/* -------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>
#include <ctime>

//! Forward declaration
namespace eos
{
class IFileMD;
}

EOSMGMNAMESPACE_BEGIN
//thread for dynamic cleaning the system for old and not used files.

class DynamicEC
{
private:
	AssistedThread mThread; /// thread for doing the clean up

	std::string mSpaceName; /// the space that the thread is running on // this have to be cheked on how it will have to run over

	std::string timeStore; /// some variable to store the time, to compare with the new time, can also be done dynamic from a function and like five years from now;

public:

	DynamicEC();

	~DynamicEC();

	void Stop();

	void CleanUp(ThreadAssistant& assistant) noexcept; /// ask for noexcept;





}



EOSMGMNAMESPACE_END
#endif
