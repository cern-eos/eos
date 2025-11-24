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
#include "common/AssistedThread.hh"
#include "mgm/FsView.hh"
#include "common/ioMonitor/proto/IoBuffer.pb.h"
// #include "mgm/XrdMgmOfs.hh"
#include <google/protobuf/util/json_util.h>
#include <unordered_map>
#include "Shaping.pb.h"

EOSMGMNAMESPACE_BEGIN

class IoShaping : public eos::common::LogId{
	private:
		AssistedThread 		_mReceivingThread;
		AssistedThread 		_mPublishingThread;
		AssistedThread 		_mShapingThread;
		mutable std::mutex	_mSyncThread;

		std::atomic<bool> 	_mReceiving;
		std::atomic<bool> 	_mPublishing;
		std::atomic<bool> 	_mShaping;

		std::unordered_map<std::string, std::string>	_shapings;

		std::atomic<size_t>	_receivingTime;

		//----------------------------------------------------------------------------
		/// Extracts the data from each node
		/// every "_receivingTime" (aka std::atomic<size_t>) second.
		///
		/// @param assistant reference to thread object
		//----------------------------------------------------------------------------
		void receive(ThreadAssistant &assistant) noexcept;

		//----------------------------------------------------------------------------
		/// Publish trafic data to each node
		/// every "_receivingTime" (aka std::atomic<size_t>) second.
		///
		/// @param assistant reference to thread object
		//----------------------------------------------------------------------------
		void publishing(ThreadAssistant &assistant);

		//----------------------------------------------------------------------------
		/// Shape
		/// every "_receivingTime" (aka std::atomic<size_t>) second.
		///
		/// @param assistant reference to thread object
		//----------------------------------------------------------------------------
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

		//----------------------------------------------------------------------------
		/// Start receiving thread
		///
		/// @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool startReceiving();

		//----------------------------------------------------------------------------
		/// Stop receiving thread
		///
		/// @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool stopReceiving();

		//----------------------------------------------------------------------------
		/// Start publising thread
		///
		/// @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool startPublishing();

		//----------------------------------------------------------------------------
		/// Stop publising thread
		///
		/// @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool stopPublishing();

		//----------------------------------------------------------------------------
		/// Start shaping thread
		///
		/// @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool startShaping();

		//----------------------------------------------------------------------------
		/// Stop shaping thread
		///
		/// @return true if successful, otherwise false
		//----------------------------------------------------------------------------
		bool stopShaping();

		void setReceivingTime(size_t);
};

EOSMGMNAMESPACE_END
