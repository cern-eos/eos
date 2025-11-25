//  File: IoMap.hh
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

#include "IoStat.hh"

//--------------------------------------------
/// The current name of the class when us
/// printInfo function
//--------------------------------------------
#define IOMAP_NAME "IoMap"

//--------------------------------------------
/// The time the cleanloop function must wait
/// before cleaning the map
//--------------------------------------------
#define TIME_TO_CLEAN 60

class IoMap {
	private:
		//--------------------------------------------
		/// @brief Multithreaded function to clean up
		/// inactive IoStats
		///
		/// @details
		/// Checks for inactive I/Os every N seconds
		/// (60s by default) and removes them
		//--------------------------------------------
		void cleanerLoop();

		//--------------------------------------------
		/// Main variable that keeps track of all IoStats
		//--------------------------------------------
		std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> > _filesMap;

		//--------------------------------------------
		/// Keeps all app name
		//--------------------------------------------
		std::unordered_set<std::string> _apps;

		//--------------------------------------------
		/// Keeps all users id
		//--------------------------------------------
		std::unordered_set<uid_t> _uids;

		//--------------------------------------------
		/// Keeps all groups id
		//--------------------------------------------
		std::unordered_set<gid_t> _gids;

		//--------------------------------------------
		/// Variables to manage multithreading
		//--------------------------------------------
		mutable std::mutex _mutex;
		std::thread _cleaner;
		std::atomic<bool> _running;
		std::condition_variable _cv;

		//--------------------------------------------
		/// @brief Displays the string given as a parameter
		/// in a format corresponding to the class with the
		/// current timestamp
		///
		/// @param os The output stream
		/// @param msg The message to display
		//--------------------------------------------
		void	printInfo(std::ostream &os, const char *) const;

		//--------------------------------------------
		/// @brief Displays the string given as a parameter
		/// in a format corresponding to the class with the
		/// current timestamp
		///
		/// @param	os	The output stream
		/// @param	msg	The message to display
		//--------------------------------------------
		void	printInfo(std::ostream &os, const std::string &) const;

	public:
		//--------------------------------------------
		/// Orthodoxe canonical form
		//--------------------------------------------

		//--------------------------------------------
		/// Main constructor
		//--------------------------------------------
		IoMap();

		//--------------------------------------------
		/// Destructor
		//--------------------------------------------
		~IoMap();

		//--------------------------------------------
		/// Constructor by copy constructor
		//--------------------------------------------
		IoMap(const IoMap &other);

		//--------------------------------------------
		/// Overload the operator =
		//--------------------------------------------
		IoMap& operator=(const IoMap &other);

		//--------------------------------------------
		/// @brief Optional constructor
		///
		/// @details
		/// If the constructor is called with any int,
		/// the multithreaded cleanLoop function will
		/// not be run, making debugging easier.
		///
		/// @param	int ignored
		//--------------------------------------------
		IoMap(int);

		//--------------------------------------------
		/// Public static mutex to share outputs stream
		//--------------------------------------------
		static std::mutex _osMutex;
	
		//--------------------------------------------
		/// @brief Adds an IoStat object to the multimap
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
		/// @brief Adds an IoStat object to the multimap
		/// with the corresponding elements
		///
		/// @param	inode	File id
		/// @param	app		Name of the application
		/// @param	uid		ID of the corresponding user
		/// @param	gid		ID of the corresponding group
		/// @param	rbytes	Number of bytes read
		//--------------------------------------------
		void addWrite(uint64_t inode, const std::string &app, uid_t uid, gid_t gid, size_t wbytes);

		//--------------------------------------------
		/// @brief Delete the appName of 
		/// the _apps variable
		///
		/// @param appName name of the tracked app
		///
		/// @return bool if the appName has been deleted
		//--------------------------------------------
		bool rm(std::string &appName);

		//--------------------------------------------
		/// @brief Delete the uid/gid of 
		/// the _uids/gids variable
		///
		/// @param type Type of the id variable
		/// @param id The uid/gid to delete
		///
		/// @return bool if the id has been deleted
		//--------------------------------------------
		bool rm(io::TYPE type, size_t id);

		//--------------------------------------------
		///@brief Get all apps
		///
		/// @return std::vector<std::string> Vector of
		/// all current active apps
		//--------------------------------------------
		std::vector<std::string> getApps() const;

		//--------------------------------------------
		///@brief Get all uids
		///
		/// @return std::vector<uid_t> Vector of
		/// all current active uids
		//--------------------------------------------
		std::vector<uid_t> getUids() const;

		//--------------------------------------------
		///@brief Get all gids
		///
		/// @return std::vector<uid_t> Vector of
		/// all current active gids
		//--------------------------------------------
		std::vector<gid_t> getGids() const;
		
		//--------------------------------------------
		///@brief Get a copy of the multimap
		///
		/// @return std::unordered_multimap<uint64_t, 
		/// std::shared_ptr<IoStat> >
		//--------------------------------------------
		std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> > GetAllStatsSnapshot() const;

		//--------------------------------------------
		/// @brief Overload operator << to print
		/// the entire multimap from a IoMap object
		//--------------------------------------------
		friend std::ostream& operator<<(std::ostream &os, const IoMap &other);

		//--------------------------------------------
		/// @brief Returns a iterator that points to the first
    	/// element in the %unordered_multimap.
		///
		/// @return std::unordered_multimap<uint64_t, 
		/// std::shared_ptr<IoStat> >::iterator
		//--------------------------------------------
		std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> >::iterator begin();

		//--------------------------------------------
		/// @brief Returns a iterator that points to the last
    	/// element in the %unordered_multimap.
		///
		/// @return std::unordered_multimap<uint64_t, 
		/// std::shared_ptr<IoStat> >::iterator
		//--------------------------------------------
		std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> >::iterator end();

		//--------------------------------------------
		/// @brief Calculates the weighted average and
		/// standard deviation
		/// 
		/// @details
		/// Takes as parameters a map reference of std::pair
		/// containing the average and standard deviations
		/// as well as the size of the total number of elements
		/// 
		/// @param indexData the map that will be used to
		/// calculate the weighted average and standard deviation
		///
		/// @return std::pair<double, double> first is
		/// the average, second the standard deviation
		//--------------------------------------------
		std::pair<double, double> calculeWeighted(std::map<std::pair<double, double>, size_t> &indexData) const;

		//--------------------------------------------
		/// Template
		/// @brief Get the WRITE or READ bandwidth
		///
		/// @details
		/// The function calculates the READ or WRITE
		/// weighted bandwidth (depending on the enumMark
		/// variable given as a parameter) during the
		/// last N seconds
		///
		/// @param	type Allows to keep the context
		/// if you want the uid or gid
		/// @param	index Template variable
		/// index type can be const char*/std::string.
		/// Calculates the weighted bandwidth according to
		/// the type of the variable and get the data from
		/// all the corresponding I/Os
		/// @param	enumMark READ or WRITE variable comes
		/// from the IoStat::Marks enumerator
		/// @param seconds(optional) The second range during
		/// the last N I/O from now (by default - 10s)
		///
		/// @return std::nullopt If an error is encountered
		///
		/// @return std::optional<std::pair<double, double>>
		/// first is the weighted average, second is the
		/// weighted standard deviation 
		//--------------------------------------------
		template <typename T>
		std::optional<std::pair<double, double> > getBandwidth(io::TYPE type, const T index, IoStat::Marks enumMark, size_t seconds = 10) const{
			std::lock_guard<std::mutex> lock(_mutex);
			if ((enumMark != IoStat::Marks::READ && enumMark != IoStat::Marks::WRITE)
				|| (type != io::TYPE::GID && type != io::TYPE::UID))
				return std::nullopt;
			else if (seconds == 0)
				return (std::pair<double, double>(0, 0));

			std::map<std::pair<double, double>, size_t> indexData;
			std::pair<double, double> tmp = {0, 0};
			size_t size = 0;

			/// Check the type of the index variable and store necessary bandwidth
			if (type == io::TYPE::UID || type == io::TYPE::GID){
				for (auto it : _filesMap){
					if ((type == io::TYPE::UID && it.second->getUid() == static_cast<uid_t>(index))
					|| (type == io::TYPE::GID && it.second->getGid() == static_cast<gid_t>(index))){
						tmp = it.second->bandWidth(enumMark, &size, seconds);
						indexData.insert({tmp, size});
						size = 0;
						tmp = {0, 0};
					}
					else
						continue;
				}
			}
			else
				return std::nullopt;
			if (indexData.size() <= 0)
				return std::nullopt;

			/// Calcule weighted average/standard deviation
			return (calculeWeighted(indexData));
		}

		//--------------------------------------------
		/// Template
		/// @brief Get the WRITE or READ bandwidth
		///
		/// @details
		/// The function calculates the READ or WRITE
		/// weighted bandwidth (depending on the enumMark
		/// variable given as a parameter) during the
		/// last N seconds
		///
		/// @param index Template variable
		/// index type can be uid_t/gid_t.
		/// Calculates the weighted bandwidth according to
		/// the type of the variable and get the data from
		/// all the corresponding I/Os
		/// @param	enumMark READ or WRITE variable comes
		/// from the IoStat::Marks enumerator
		/// @param seconds(optional) The second range during
		/// the last N I/O from now (by default - 10s)
		///
		/// @return std::nullopt If an error is encountered
		/// @return std::optional<std::pair<double, double>>
		/// first is the weighted average, second is the
		/// weighted standard deviation 
		//--------------------------------------------
		template <typename T>
		std::optional<std::pair<double, double> > getBandwidth(const T index, IoStat::Marks enumMark, size_t seconds = 10) const{
			std::lock_guard<std::mutex> lock(_mutex);

			if (enumMark != IoStat::Marks::READ && enumMark != IoStat::Marks::WRITE)
				return std::nullopt;
			else if (seconds == 0)
				return (std::pair<double, double>(0, 0));

			std::map<std::pair<double, double>, size_t> indexData;
			std::pair<double, double> tmp = {0, 0};
			size_t size = 0;

			/// Check the type of the index variable and store necessary bandwidth
			if (std::is_same_v<T, std::string> || std::is_same_v<T, const char *>){
				std::string id(index);
				for (auto it : _filesMap){
					if (it.second->getApp() == id){
						tmp = it.second->bandWidth(enumMark, &size, seconds);
						indexData.insert({tmp, size});
						size = 0;
						tmp = {0, 0};
					}
				}
			} else {
				if (io::IoMapDebug)
					printInfo(std::cerr, "No match found for data type");
				return std::nullopt;
			}
			if (indexData.size() <= 0)
				return std::nullopt;

			/// Calcule weighted average/standard deviation
			return (calculeWeighted(indexData));
		}

	
		//--------------------------------------------
		/// Template
		/// @brief Get a bandwidth summary over the
		/// last N seconds (10s by default)
		///
		/// @param type Allows to keep the context
		/// if you want the uid or gid
		/// @param index Template variable
		/// index type can be uid_t/gid_t.
		/// Calculates the weighted bandwidth according to
		/// the type of the variable and get the data from
		/// all the corresponding I/Os
		/// @param seconds(optional) The second range during
		/// the last N I/O from now (by default - 10s)
		///
		/// @return std::nullopt If an error is encountered
		/// or the summary is completely empty
		/// @return std::optional<IoStatSummary>
		/// A summary of write and read bandwidth
		/// over the last N seconds.
		//--------------------------------------------
		template <typename T>
		std::optional<IoStatSummary> getSummary(io::TYPE type, const T index, size_t seconds = 10) const{
			std::lock_guard<std::mutex> lock(_mutex);

			if (io::IoMapDebug)
				printInfo(std::cout, "GetSummary for " + std::to_string(index));

			if (type != io::TYPE::GID && type != io::TYPE::UID)
				return std::nullopt;

			std::map<std::pair<double, double>, size_t> readData;
			std::map<std::pair<double, double>, size_t> writeData;
			size_t size = 0;

			IoStatSummary summary;

			/// Check the type of the index variable and store necessary bandwidth
			if (type == io::TYPE::UID || type == io::TYPE::GID){
				for (auto it : _filesMap){
					if ((type == io::TYPE::UID && it.second->getUid() == static_cast<uid_t>(index))
					|| (type == io::TYPE::GID && it.second->getGid() == static_cast<gid_t>(index))){
						/// Get read bandwidth and size
						readData.insert({it.second->bandWidth(IoStat::Marks::READ, &size, seconds), size});
						summary.rSize += size;
						summary.rIops += it.second->getIOPS(IoStat::Marks::READ, seconds) * size;
						size = 0;

						/// Get write bandwidth and size
						writeData.insert({it.second->bandWidth(IoStat::Marks::WRITE, &size, seconds), size});
						summary.wSize += size;
						summary.wIops += it.second->getIOPS(IoStat::Marks::WRITE, seconds) * size;
						size = 0;
					}
				}
			} else {
				if (io::IoMapDebug)
					printInfo(std::cerr, "No match found for data type");
				return std::nullopt;
			}

			/// Check empty I/O case
			if (writeData.size() <= 0 && readData.size() <= 0)
				return std::nullopt;
			if (writeData.size() <= 0)
				summary.writeBandwidth = std::nullopt;
			if (readData.size() <= 0)
				summary.readBandwidth = std::nullopt;

			/// Calcule read/write weighted average and standard deviation
			if (summary.readBandwidth.has_value())
				summary.readBandwidth = calculeWeighted(readData);
			if (summary.writeBandwidth.has_value())
				summary.writeBandwidth = calculeWeighted(writeData);

			/// Calcule read/write IOPS
			if (summary.rSize > 0)
				summary.rIops /= summary.rSize;
			if (summary.wSize > 0)
				summary.wIops /= summary.wSize;

			if (io::IoMapDebug)
				printInfo(std::cout, "GetSummary succeeded");

			return summary;
		}

		//--------------------------------------------
		/// Template
		/// @brief Get a bandwidth summary over the
		/// last N seconds (10s by default)
		///
		/// @param index Template variable
		/// index type can be const char*/std::string.
		/// Calculates the weighted bandwidth according to
		/// the type of the variable and get the data from
		/// all the corresponding I/Os
		/// @param seconds(optional) The second range during
		/// the last N I/O from now (by default - 10s)
		///
		/// @return std::nullopt If an error is encountered
		/// or the summary is completely empty
		/// @return std::optional<IoStatSummary>
		/// A summary of write and read bandwidth
		/// over the last N seconds.
		//--------------------------------------------
		template <typename T>
		std::optional<IoStatSummary> getSummary(const T index, size_t seconds = 10) const{
			std::lock_guard<std::mutex> lock(_mutex);

			if (io::IoMapDebug)
				printInfo(std::cout, "GetSummary for " + std::string(index));
			if (seconds == 0)
				return (IoStatSummary());

			std::map<std::pair<double, double>, size_t> readData;
			std::map<std::pair<double, double>, size_t> writeData;
			size_t size = 0;

			IoStatSummary summary;

			/// Check the type of the index variable and store necessary bandwidth
			if (std::is_same_v<T, std::string> || std::is_same_v<T, const char *>){
				std::string id(index);
				for (auto it : _filesMap){
					if (it.second->getApp() == id){
						/// Get read bandwidth/IOPS and size
						readData.insert({it.second->bandWidth(IoStat::Marks::READ, &size, seconds), size});
						summary.rSize += size;
						summary.rIops += it.second->getIOPS(IoStat::Marks::READ, seconds) * size;
						size = 0;

						/// Get write bandwidth/IOPS and size
						writeData.insert({it.second->bandWidth(IoStat::Marks::WRITE, &size, seconds), size});
						summary.wSize += size;
						summary.wIops += it.second->getIOPS(IoStat::Marks::WRITE, seconds) * size;
						size = 0;
					}
				}
			} else{
				if (io::IoMapDebug)
					printInfo(std::cerr, "No match found for data type");
				return std::nullopt;
			}

			/// Check empty I/O case
			if (writeData.size() <= 0 && readData.size() <= 0)
				return std::nullopt;
			if (writeData.size() <= 0)
				summary.writeBandwidth = std::nullopt;
			if (readData.size() <= 0)
				summary.readBandwidth = std::nullopt;

			/// Calcule read/write weighted average and standard deviation
			if (summary.readBandwidth.has_value())
				summary.readBandwidth = calculeWeighted(readData);
			if (summary.writeBandwidth.has_value())
				summary.writeBandwidth = calculeWeighted(writeData);

			/// Calcule read/write IOPS
			if (summary.rSize > 0)
				summary.rIops /= summary.rSize;
			if (summary.wSize > 0)
				summary.wIops /= summary.wSize;

			if (io::IoMapDebug)
				printInfo(std::cout, "GetSummary succeeded");

			return summary;
		}
};
