//  File: IoAggregate.hh
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

//--------------------------------------------
/// Each class has a variable define DEBUG which
/// can be defined in the IoMonitor.hh namespace
//--------------------------------------------

#pragma once

#include "IoMap.hh"

//--------------------------------------------
/// The current name of the class when us
/// printInfo function
//-----------------------------------------
#define IOAGGREGATE_NAME "IoAggregate"

class IoAggregate{
	private:

		//-----------------------------------------
		/// Structure that keeps all IoStatSummary
		/// of all appName/uid/gid that are tracked
		//-----------------------------------------
		struct Bin{
			std::unordered_multimap<std::string, IoStatSummary> appStats;
			std::unordered_multimap<uid_t, IoStatSummary> uidStats;
			std::unordered_multimap<gid_t, IoStatSummary> gidStats;
		};

		//-----------------------------------------
		/// The number of seconds the class must
		/// wait before updating
		//-----------------------------------------
		size_t _intervalSec;

		//-----------------------------------------
		/// The total track time interval
		//-----------------------------------------
		size_t _winTime;

		//-----------------------------------------
		/// The index of the object to know which Bin
		/// we are on
		//-----------------------------------------
		size_t _currentIndex;

		//-----------------------------------------
		/// The class keeps up to date with the last
		/// time it was updated
		//-----------------------------------------
		std::chrono::system_clock::time_point _currentTime;

		//-----------------------------------------
		/// set of unordered_set which is a list of
		/// all appName/uid/gid that are tracked
		//-----------------------------------------
		std::unordered_set<std::string>	_apps;
		std::unordered_set<uid_t>		_uids;
		std::unordered_set<gid_t>		_gids;

		//-----------------------------------------
		/// The buffer that keeps all the bins
		//-----------------------------------------
		std::vector<Bin> _bins;

		mutable std::mutex _mutex;

		//--------------------------------------------
		/// Deleted default constructor
		//--------------------------------------------
		IoAggregate() = delete;

		//--------------------------------------------
		/// Display the string given as parameter in
		/// specific format with the current time
		//--------------------------------------------
		static void printInfo(std::ostream &os, const std::string &msg);

		//--------------------------------------------
		/// Display the string given as parameter in
		/// specific format with the current time
		//--------------------------------------------
		static void printInfo(std::ostream &os, const char *msg);

	public:
		//--------------------------------------------
		/// Orthodoxe canonical form
		//--------------------------------------------

		//--------------------------------------------
		/// Destructor
		//--------------------------------------------
		~IoAggregate();

		//--------------------------------------------
		/// Constructor by copy constructor
		//--------------------------------------------
		IoAggregate(const IoAggregate &other);

		//--------------------------------------------
		/// Overload the operator =
		//--------------------------------------------
		IoAggregate& operator=(const IoAggregate &other);

		//--------------------------------------------
		/// Main constructor
		//--------------------------------------------
		IoAggregate(size_t winTime);

		//--------------------------------------------
		/// @brief Overload operator << to print
		/// a IoAggregate object
		//--------------------------------------------
		friend std::ostream& operator<<(std::ostream &os, const IoAggregate &other);

		//--------------------------------------------
		/// @brief Updates the current bin according to the
		/// appName/uid/gid which are tracked every N seconds
		/// (depending on _intervalSec)
		///
		/// @param maps A reference to an ioMap object to
		/// extract the summary from it
		//--------------------------------------------
		void update(const IoMap &maps);

		//--------------------------------------------
		/// @brief Add a Bin object, and set the index
		/// to that Bin
		///
		/// @return -1 If an error is encountered
		/// @return the position of the new index
		//--------------------------------------------
		int shiftWindow();

		//--------------------------------------------
		/// @brief Changes the position of the current
		/// index (_currentIndex)
		///
		/// @param index The new future position of the index
		/// does nothing if it goes out of range
		///
		/// @return -1 If an error is encountered
		/// @return the position of the new index
		//--------------------------------------------
		int shiftWindow(const size_t index);
	
		//--------------------------------------------
		/// @brief Condenses a vector of IoStatSummary
		/// into a single one
		///
		/// @param summarys A vector of summary
		///
		/// @return std::nullopt If an error is encountered
		/// or the summary is completely empty else
		/// a IoStatSummary
		//--------------------------------------------
		static std::optional<IoStatSummary> summaryWeighted(const std::vector<IoStatSummary> &summarys, size_t winTime = 0);

		//--------------------------------------------
		/// Template
		/// @brief Add an app name to the set that
		/// would be tracked
		///
		/// @param index Template variable
		/// The new tracked app Name
		///
		/// @return -1 If the variable type doesn't match
		/// @return 0 If the track has been set correctly
		//--------------------------------------------
		template <typename T>
		int setTrack(T index){
			if (!(std::is_same_v<T, std::string> || std::is_same_v<T, const char *>))
				return -1;
			_apps.insert(std::string(index));
			return 0;
		}

		//--------------------------------------------
		/// Template
		/// @brief Add an uid/gid to the set that
		/// would be tracked
		///
		/// @param type Allows to keep the context
		/// if you want the uid or gid
		/// @param index Template variable
		/// The new tracked app Name
		///
		/// @return -1 If the type variable doesn't match
		/// @return 0 If the track has been set correctly
		//--------------------------------------------
		template <typename T>
		int setTrack(io::TYPE type, T index){
			if (type == io::TYPE::UID)
				_uids.insert(index);
			else if (type == io::TYPE::GID)
				_gids.insert(index);
			else
				return -1;
			return 0;
		}

		//--------------------------------------------
		/// Template
		/// @brief Add an IoStatSummary to the current Bin
		///
		/// @details Adds an ioStatSummary to the current
		/// Bin, maintaining a circular buffer of size
		/// "_winTime / _intervaleSec". This means that if
		/// the Bin is full, the oldest Summary will be
		/// overwritten.
		///
		/// @param type Allows to keep the context
		/// if you want the uid or gid
		/// @param index Template variable
		/// index type can be uid_t/gid_t
		/// @param summary The summary to add
		//--------------------------------------------
		template <typename T>
		void addSample(io::TYPE type, const T index, IoStatSummary &summary){

			if constexpr (io::IoAggregateDebug)
				printInfo(std::cout, "add sample for " + std::to_string(index));
			if (type == io::TYPE::UID){
				auto &uid = _bins.at(_currentIndex).uidStats;
				auto range = uid.equal_range(index);
				if (static_cast<size_t>(std::distance(range.first, range.second)) >= _winTime / _intervalSec){
					struct timespec now;
					clock_gettime(CLOCK_REALTIME, &now);
					for (auto it = range.first; range.first != range.second;){
						if (difftime(now.tv_sec, range.first->second.io_time.tv_sec) > difftime(now.tv_sec, it->second.io_time.tv_sec))
							it = range.first;
						range.first++;
						if (range.first == range.second)
							uid.erase(it);
					}
				}
				uid.insert({index, summary});
				if constexpr (io::IoAggregateDebug)
					printInfo(std::cout, "add uid sample succeedded");
			}
			else if (type == io::TYPE::GID){
				auto &gid = _bins.at(_currentIndex).gidStats;
				auto range = gid.equal_range(index);
				if (static_cast<size_t>(std::distance(range.first, range.second)) >= _winTime / _intervalSec){
					struct timespec now;
					clock_gettime(CLOCK_REALTIME, &now);
					for (auto it = range.first; range.first != range.second;){
						if (difftime(now.tv_sec, range.first->second.io_time.tv_sec) > difftime(now.tv_sec, it->second.io_time.tv_sec))
							it = range.first;
						range.first++;
						if (range.first == range.second)
							gid.erase(it);
					}
				}
				gid.insert({index, summary});
				if constexpr (io::IoAggregateDebug)
					printInfo(std::cout, "add gid sample succeedded");
			}
		}

		//--------------------------------------------
		/// Template
		/// @brief Add an IoStatSummary to the current Bin
		///
		/// @details Adds an ioStatSummary to the current
		/// Bin, maintaining a circular buffer of size
		/// "_winTime / _intervaleSec". This means that if
		/// the Bin is full, the oldest Summary will be
		/// overwritten.
		///
		/// @param index Template variable
		/// index type can const char*/std::string
		/// @param summary The summary to add
		//--------------------------------------------
		template <typename T>
		void addSample(const T index, IoStatSummary &summary){

			if constexpr (io::IoAggregateDebug)
				printInfo(std::cout, "add Sample for " + std::string(index));
			if (std::is_same_v<T, std::string> || std::is_same_v<T, const char *>){
				auto &app = _bins.at(_currentIndex).appStats;
				auto range = app.equal_range(index);
				if (static_cast<size_t>(std::distance(range.first, range.second)) >= _winTime / _intervalSec){
					struct timespec now;
					clock_gettime(CLOCK_REALTIME, &now);
					for (auto it = range.first; range.first != range.second;){
						if (difftime(now.tv_sec, range.first->second.io_time.tv_sec) > difftime(now.tv_sec, it->second.io_time.tv_sec))
							it = range.first;
						range.first++;
						if (range.first == range.second)
							app.erase(it);
					}
				}
				app.insert({std::string(index), summary});
				if constexpr (io::IoAggregateDebug)
					printInfo(std::cout, "add app sample succeedded");
			}
		}

		//--------------------------------------------
		/// Get current index
		//--------------------------------------------
		size_t getIndex() const;

		//--------------------------------------------
		/// Get available apps
		//--------------------------------------------
		std::vector<std::string> getApps() const;

		//--------------------------------------------
		/// Get available uids
		//--------------------------------------------
		std::vector<uid_t> getUids() const;

		//--------------------------------------------
		/// Get available gids
		//--------------------------------------------
		std::vector<gid_t> getGids() const;

		//--------------------------------------------
		/// Template
		/// @brief Get the appName index summary
		///
		/// @param index Template variable
		/// index type can be const char*/std::string
		///
		/// @return std::nullopt If an error is encountered
		/// or the summary is completely empty
		/// @return std::optional<IoStatSummary>
		/// Total index summary of the window
		//--------------------------------------------
		template <typename T>
		std::optional<IoStatSummary> getSummary(const T index){
			std::vector<IoStatSummary> summarys;

			if constexpr (io::IoAggregateDebug)
				printInfo(std::cout, "get Summary for " + std::string(index));

			if (!(std::is_same_v<T, std::string> || std::is_same_v<T, const char *>)
			|| _apps.find(index) == _apps.end())
				return std::nullopt;

			/// Fill in the corresponding summary vector
			auto &it = _bins.at(_currentIndex);
			for (auto appsSumarrys : it.appStats)
				if (appsSumarrys.first == index)
					summarys.emplace_back(appsSumarrys.second);

			if constexpr (io::IoAggregateDebug)
				printInfo(std::cout, "get Summary succeeded");

			return summaryWeighted(summarys);
		}

		//--------------------------------------------
		/// Template
		/// @brief Get the uid/gid index summary
		///
		/// @param type Allows to keep the context
		/// if you want the uid or gid
		/// @param index Template variable
		/// index type can be uid_t/gid_t
		///
		/// @return std::nullopt If an error is encountered
		/// or the summary is completely empty
		/// @return std::optional<IoStatSummary>
		/// Total index summary of the window
		//--------------------------------------------
		template <typename T>
		std::optional<IoStatSummary> getSummary(io::TYPE type, const T index){
			std::vector<IoStatSummary> summarys;

			if constexpr (io::IoAggregateDebug)
				printInfo(std::cout, "get Summary for " + std::to_string(index));

			if (type != io::TYPE::GID && type != io::TYPE::UID)
				return std::nullopt;

			if ((type == io::TYPE::UID && _uids.find(index) == _uids.end())
			|| (type == io::TYPE::GID && _gids.find(index) == _gids.end()))
				return std::nullopt;
			
			/// Fill in the corresponding summary vector
			auto &it = _bins.at(_currentIndex);
			if (type == io::TYPE::UID)
				for (auto uidsSumarrys : it.uidStats){
					if (uidsSumarrys.first == static_cast<uid_t>(index))
						summarys.emplace_back(uidsSumarrys.second);
			}
			else if (type == io::TYPE::GID)
				for (auto gidsSumarrys : it.gidStats)
					if (gidsSumarrys.first == static_cast<gid_t>(index))
						summarys.emplace_back(gidsSumarrys.second);

			if constexpr (io::IoAggregateDebug)
				printInfo(std::cout, "get Summary succeeded");

			return summaryWeighted(summarys);
		}
};
