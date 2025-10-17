//  File: IoShaping.hh
//  Author: Ilkay Yanar - 42Lausanne / CERN
//  ----------------------------------------------------------------------

/*************************************************************************
 *  EOS - the CERN Disk Storage System                                   *
 *  Copyright (C) 2025 CERN/Switzerland                                  *
 *                                                                       *
 *  This program is free software: you can redistribute it and/or modify *
 *  it under the terms of the GNU General Public License as published by *
 *  the Free Software Foundation, either version 3 of the License, or    *
 *  (at your option) any later version.                                  *
 *                                                                       *
 *  This program is distributed in the hope that it will be useful,      *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 *  GNU General Public License for more details.                         *
 *                                                                       *
 *  You should have received a copy of the GNU General Public License    *
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 *************************************************************************/

#pragma once

#include "mgm/Namespace.hh"
#include "common/ioMonitor/include/IoAggregate.hh"
#include "common/AssistedThread.hh"
// #include "common/Logging.hh"
#include "mgm/XrdMgmOfs.hh"
#include <unordered_map>

namespace eos::common
{
class TransferQueue;
}

EOSMGMNAMESPACE_BEGIN

//--------------------------------------------
/// The current name of the class when us
/// printInfo function
//--------------------------------------------
#define IOAGGREGATEMAP_NAME "IoShaping"

class IoShaping : public eos::common::LogId{
	private:
		AssistedThread 		_mReceivingThread;
		AssistedThread 		_mPublishingThread;
		AssistedThread 		_mShapingThread;
		mutable std::mutex	_mSyncThread;

		std::atomic<bool> 	_rReceiving;
		std::atomic<bool> 	_rPublishing;
		std::atomic<bool> 	_rShaping;

		std::unordered_map<std::string, double> _shapings;

		std::atomic<size_t>	_receivingTime;

		//----------------------------------------------------------------------------
		//! Start receiving thread
		//!
		//! @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool startReceiving();

		//----------------------------------------------------------------------------
		//! Stop receiving thread
		//!
		//! @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool stopReceiving();

		//----------------------------------------------------------------------------
		//! Start publising thread
		//!
		//! @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool startPublishing();

		//----------------------------------------------------------------------------
		//! Stop publising thread
		//!
		//! @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool stopPublishing();

		//----------------------------------------------------------------------------
		//! Start shaping thread
		//!
		//! @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool startShaping();

		//----------------------------------------------------------------------------
		//! Stop shaping thread
		//!
		//! @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool stopShaping();

		void receive(ThreadAssistant &assistant) noexcept;

		void publish(ThreadAssistant &assistant) noexcept;

		void shaping(ThreadAssistant &assistant) noexcept;
		
	public:
		//--------------------------------------------
		/// Orthodoxe canonical form
		//--------------------------------------------

		//--------------------------------------------
		/// Main constructor
		//--------------------------------------------
		IoShaping(size_t = 5);

		//--------------------------------------------
		/// Destructor
		//--------------------------------------------
		~IoShaping();

		//--------------------------------------------
		/// Constructor by copy constructor
		//--------------------------------------------
		IoShaping(const IoShaping &other);

		//--------------------------------------------
		/// Overload the operator =
		//--------------------------------------------
		IoShaping& operator=(const IoShaping &other);

		void setReceivingTime(size_t);
};

EOSMGMNAMESPACE_END
