//  File: IoAggregateMap.hh
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

#include "IoAggregate.hh"

//--------------------------------------------
/// The time the updateAggregateLoop function
/// must wait before updating all IoAggregate object
//--------------------------------------------
#define TIME_TO_UPDATE 1

//--------------------------------------------
/// The current name of the class when us
/// printInfo function
//--------------------------------------------
#define IOAGGREGATEMAP_NAME "IoAggregateMap"

class IoAggregateMap{
	private:
		//--------------------------------------------
		/// @brief Multithreaded function update all
		/// IoAggregate object in _aggregates variable
		/// every N seconds (1s by default)
		//--------------------------------------------
		void updateAggregateLoop();

		//--------------------------------------------
		/// Wrapped map
		//--------------------------------------------
		IoMap _map;

		//--------------------------------------------
		/// Main variable that keeps window of all IoAggregate
		//--------------------------------------------
		std::unordered_map<size_t, std::unique_ptr<IoAggregate> > _aggregates;

		//--------------------------------------------
		/// Variables to manage multithreading
		//--------------------------------------------
		std::thread _thread;
		std::atomic<bool> _running;
		std::condition_variable _cv;
		mutable std::mutex _mutex;

		//--------------------------------------------
		/// Display the string given as parameter in
		/// specific format with the current time
		//--------------------------------------------
		void printInfo(std::ostream &os, const char *msg) const;

		//--------------------------------------------
		/// Display the string given as parameter in
		/// specific format with the current time
		//--------------------------------------------
		void printInfo(std::ostream &os, const std::string &msg) const;

	public:
		//--------------------------------------------
		/// Orthodoxe canonical form
		//--------------------------------------------

		//--------------------------------------------
		/// Main constructor
		//--------------------------------------------
		IoAggregateMap();

		//--------------------------------------------
		/// Destructor
		//--------------------------------------------
		~IoAggregateMap();

		//--------------------------------------------
		/// Constructor by copy constructor
		//--------------------------------------------
		IoAggregateMap(const IoAggregateMap &other);

		//--------------------------------------------
		/// Overload the operator =
		//--------------------------------------------
		IoAggregateMap& operator=(const IoAggregateMap &other);

		//--------------------------------------------
		/// Overload the operator []
		//--------------------------------------------
		std::unique_ptr<IoAggregate>& operator[](size_t);

		//--------------------------------------------
		/// @brief Optional constructor
		///
		/// @details
		/// If the constructor is called with any int,
		/// the multithreaded updateAggregateLoop function
		/// will not be run, making debugging easier.
		///
		/// @param	int ignored
		//--------------------------------------------
		IoAggregateMap(int);


		//--------------------------------------------
		/// @brief Adds an IoStat object to the map
		/// with the corresponding elements
		///
		/// @param	inode	File id
		/// @param	app		Name of the application
		/// @param	uid		ID of the corresponding user
		/// @param	gid		ID of the corresponding group
		/// @param	rbytes	Number of bytes read
		//--------------------------------------------
		void addRead(uint64_t inode, const std::string &app, uid_t uid, gid_t gid, size_t rbytes);

		//--------------------------------------------
		/// @brief Adds an IoStat object to the map
		/// with the corresponding elements
		///
		/// @param	inode	file id
		/// @param	app		Name of the application
		/// @param	uid		ID of the corresponding user
		/// @param	gid		ID of the corresponding group
		/// @param	rbytes	Number of bytes read
		//--------------------------------------------
		void addWrite(uint64_t inode, const std::string &app, uid_t uid, gid_t gid, size_t wbytes);

		//--------------------------------------------
		/// @brief	Add a new window time to the aggregatation
		///
		/// @details
		/// The function adds an aggregation window of N seconds
		/// (minimum 10s) which extracts every 10s a summary of
		/// the I/O of all the appname/uid/gid which track.
		/// The IoAggregate buffer is of size winTime / 10.
		/// The buffer is circular, which means that if it
		/// fills up, the oldest entries will be overwritten.
		///
		/// @param	winTime The number of seconds of
		/// window monitoring
		///
		/// @return -1 If an error is encountered
		/// @return 0 All other cases
		//--------------------------------------------
		int addWindow(size_t winTime);

		//--------------------------------------------
		/// @brief Delete the specified window
		///
		/// @param winTime The targeted window
		///
		/// @return bool if the window has been deleted
		/// 
		//--------------------------------------------
		bool rm(size_t winTime);

		//--------------------------------------------
		/// @brief Delete the appName of 
		/// the specified window
		///
		/// @param winTime The targeted window
		/// @param appName name of the tracked app
		///
		/// @return bool if the appName has been deleted
		//--------------------------------------------
		bool rm(size_t winTime, std::string &appName);

		//--------------------------------------------
		/// @brief Delete the uid/gid of 
		/// the specified window
		///
		/// @param winTime The targeted window
		/// @param type Type of the id variable
		/// @param id The uid/gid to delete
		///
		/// @return bool if the id has been deleted
		//--------------------------------------------
		bool rm(size_t winTime, io::TYPE type, size_t id);

		//--------------------------------------------
		/// @brief Returns a vector of all available windows
		///
		/// @return std::nullopt
		/// If the _aggregation window is empty
		/// @return std::optional<std::vector<size_t> >
		//--------------------------------------------
		std::optional<std::vector<size_t> > getAvailableWindows() const;

		//--------------------------------------------
		/// @brief Returns a reference to the IoMap object
		//--------------------------------------------
		const IoMap& getIoMap() const;

		//--------------------------------------------
		/// Get available apps
		///
		/// @param winTime The targeted window
		//--------------------------------------------
		std::vector<std::string> getApps(size_t wintime) const;

		//--------------------------------------------
		/// Get available uids
		///
		/// @param winTime The targeted window
		//--------------------------------------------
		std::vector<uid_t> getUids(size_t wintime) const;

		//--------------------------------------------
		/// Get available gids
		///
		/// @param winTime The targeted window
		//--------------------------------------------
		std::vector<gid_t> getGids(size_t wintime) const;

		//--------------------------------------------
		/// @brief Return true if the window exists,
		/// otherwise returns false
		///
		/// @param winTime The targeted window
		///
		/// @return true if the window exist
		//--------------------------------------------
		bool containe(size_t winTime) const;

		//--------------------------------------------
		/// @brief Return true if the track exists in the window,
		/// otherwise returns false
		///
		/// @param winTime The targeted window
		/// @param app the app name
		///
		/// @return true if the appName in the specified
		/// window exist
		//--------------------------------------------
		bool containe(size_t winTime, std::string appName) const;

		//--------------------------------------------
		/// @brief Return true if the window exists,
		/// otherwise returns false
		///
		/// @param winTime The targeted window
		///
		/// @return true if the uid/gid in the specified
		/// window exist
		//--------------------------------------------
		bool containe(size_t winTime, io::TYPE type, size_t id) const;

		//--------------------------------------------
		/// @brief Returns a iterator that points to the
    	/// first element in the %IoMap.
		///
		/// @return std::unordered_map<uint64_t, 
		/// std::shared_ptr<IoStat> >::iterator
		//--------------------------------------------
		std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> >::iterator begin();

		//--------------------------------------------
		/// @brief Returns a iterator that points to the
    	/// last element in the %IoMap.
		///
		/// @return std::unordered_map<uint64_t, 
		/// std::shared_ptr<IoStat> >::iterator
		//--------------------------------------------
		std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> >::iterator end();

		//--------------------------------------------
		/// @brief Add a Bin object to the window's
		/// IoAggregate, and set the window's index
		/// to that Bin
		///
		/// @param winTime The targeted window
		///
		/// @return -1 If an error is encountered
		/// @return the position of the new index
		//--------------------------------------------
		int shiftWindow(size_t winTime);

		//--------------------------------------------
		/// @brief Changes the position of the index in
		/// the IoAggregate object  of the specified window
		///
		/// @param winTime The targeted window
		/// @param index The new future position of the index
		/// does nothing if it goes out of range
		///
		/// @return -1 If an error is encountered
		/// @return the position of the new index
		//--------------------------------------------
		int shiftWindow(size_t winTime, size_t index);

		//--------------------------------------------
		/// @brief Overload operator << to print
		/// all window and IoAggregate object from
		/// the %unordored_map _aggregation
		//--------------------------------------------
		friend std::ostream& operator<<(std::ostream &os, const IoAggregateMap &other);

		//--------------------------------------------
		/// Template
		/// @brief Add an app name that would be
		/// tracked to an existing window
		///
		/// @param winTime The targeted window
		/// @param index The new tracked app Name
		///
		/// @return -1 If the window does not exist
		/// @return 0 If the track has been set correctly
		//--------------------------------------------
		template <typename T>
		int setTrack(size_t winTime, const T index){
			std::lock_guard<std::mutex> lock(_mutex);
			if constexpr (io::IoAggregateMapDebug)
				printInfo(std::cout, "set appName track for " + std::string(index));
			if (_aggregates.find(winTime) == _aggregates.end()){
				if constexpr (io::IoAggregateMapDebug)
					printInfo(std::cerr, "set appName track failed");
				return -1;
			}
			_aggregates[winTime]->setTrack(index);
			if constexpr (io::IoAggregateMapDebug)
				printInfo(std::cout, "set appName track succeeded");
			return 0;
		}

		//--------------------------------------------
		/// Template
		/// @brief Add an uid/gid that would be
		/// tracked to an existing window
		///
		/// @param winTime The targeted window
		/// @param type Allows to keep the context
		/// if you want the uid or gid
		/// @param index The new tracked uid/gid
		///
		/// @return -1 If the window does not exist
		/// @return 0 If the track has been set correctly
		//--------------------------------------------
		template <typename T>
		int setTrack(size_t winTime, io::TYPE type, const T index){
			std::lock_guard<std::mutex> lock(_mutex);
			if constexpr (io::IoAggregateMapDebug)
				printInfo(std::cout, "set id track for " + std::to_string(index));
			if (_aggregates.find(winTime) == _aggregates.end()){
				if constexpr (io::IoAggregateMapDebug)
					printInfo(std::cerr, "set id track failed");
				return -1;
			}
			_aggregates[winTime]->setTrack(type, index);
			if constexpr (io::IoAggregateMapDebug)
				printInfo(std::cout, "set id track succeeded");
			return 0;
		}


		//--------------------------------------------
		/// Template
		/// @brief Get the total index summary in the
		/// specified window
		///
		/// @param winTime The targeted window
		/// @param index Template variable
		/// index type can be const char*/std::string.
		///
		/// @return std::nullopt If an error is encountered
		/// or the summary is completely empty
		/// @return std::optional<IoStatSummary>
		/// Total index summary of the window
		//--------------------------------------------
		template<typename T>
		std::optional<IoStatSummary> getSummary(size_t winTime, const T index){
			std::lock_guard<std::mutex> lock(_mutex);
			if (_aggregates.find(winTime) == _aggregates.end())
				return std::nullopt;
			return (_aggregates[winTime]->getSummary(index));
		}

		//--------------------------------------------
		/// Template
		/// @brief Get the total index summary in the
		/// specified window
		///
		/// @param winTime The targeted window
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
		template<typename T>
		std::optional<IoStatSummary> getSummary(size_t winTime, io::TYPE type, const T index){
			std::lock_guard<std::mutex> lock(_mutex);
			if (_aggregates.find(winTime) == _aggregates.end())
				return std::nullopt;
			return (_aggregates[winTime]->getSummary(type, index));
		}
};
