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
#include "ioMonitor/include/IoMonitor.hh"
#include <google/protobuf/util/json_util.h>
#include <unordered_map>
#include "Shaping.pb.h"

EOSMGMNAMESPACE_BEGIN

class IoShaping : public eos::common::LogId{
	private:

		struct Limiter{
			std::map<std::string, size_t> rApps;
			std::map<std::string, size_t> wApps;

			std::map<uid_t, size_t> rUids;
			std::map<uid_t, size_t> wUids;

			std::map<gid_t, size_t> rGids;
			std::map<gid_t, size_t> wGids;
		};

		AssistedThread 		_mReceivingThread;
		AssistedThread 		_mPublishingThread;
		AssistedThread 		_mShapingThread;
		mutable std::mutex	_mSyncThread;

		std::atomic<bool> 	_mReceiving;
		std::atomic<bool> 	_mPublishing;
		std::atomic<bool> 	_mShaping;

		IoBuffer::summarys	_shapings;
		Shaping::Scaler		_scaler;
		Limiter				_limiter;

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

		IoBuffer::summarys aggregateSummarys(std::vector<IoBuffer::summarys> &);
		bool calculeScalerNodes();
		
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

		IoBuffer::summarys getShaping() const;

		Shaping::Scaler getScaler() const;

		Limiter getLimiter() const;

		template<typename T>
		void setLimiter(T app, size_t limits) noexcept{
			std::lock_guard<std::mutex> lock(_mSyncThread);
			_limiter.apps[app] = limits;
		}

		template<typename T>
		bool setLimiter(const io::TYPE type, T id, size_t limits) noexcept{
			std::lock_guard<std::mutex> lock(_mSyncThread);

			if (type != io::TYPE::UID && type != io::TYPE::GID)
				return false;
			if (type == io::TYPE::UID)
				_limiter.uids[id] = limits;
			else if (type == io::TYPE::GID)
				_limiter.gids[id] = limits;
			return true;
		}
};

EOSMGMNAMESPACE_END
