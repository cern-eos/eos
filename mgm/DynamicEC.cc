// ----------------------------------------------------------------------
// File: DynamicEC.cc
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

#include "mgm/DynamicEC.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/Master.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "Xrd/XrdScheduler.hh"
#include <random>
#include <cmath>

extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

#define CACHE_LIFE_TIME 300 // seconds

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

DynamicEC::DynamicEC()
{
	mThread.reset(&DynamicEC::DynamicEC this);

}

void
DynamicEC::Stop()
{
	mThread.join();
}

DynamicEC::~DynamicEC()
{
	Stop();
}

void
DynamicEC::CleanUp(ThreadAssistant& assistant) noexcept
{
	gOFS->WaitUntilNamespaceIsBooted(assistant);
	assistant.wait_for(std::chrono::seconds(10));

	while(!assistant.terminationRequested())
		///Assisting variables can be written here
	{
		/// What to do when it runs

		///can have a lock for a timeout, then needs to know where it started.



	}

wait:
	//let time pass for a notification
	assistant.wait_for(std::chrono::seconds(10));

	if (assistant.terminationRequested())
	{
		return;
	}
}




EOSMGMNAMESPACE_END
