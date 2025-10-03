//  File: IoStat.hh
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
#include "IoMonitor.hh"

//--------------------------------------------
/// The current name of the class when us
/// printInfo function
//--------------------------------------------
#define IOSTAT_NAME "IoStat"

class IoStat {
		//--------------------------------------------
		/// Enumerator that allows to keep the context
		/// of READ or WRITE to avoid duplicate functions
		//--------------------------------------------
		public: enum class Marks : uint8_t{
			READ,
			WRITE
		};

	private:
		//--------------------------------------------
		/// Each IoStat class keeps track of exactly
		/// where it came from. The file ID, the name
		/// of the app it came from, the user ID and
		/// the group ID
		//--------------------------------------------
		uint64_t	_fileId;
		std::string	_app;
		uid_t		_uid;
		gid_t		_gid;

		//--------------------------------------------
		/// Read deque that keeps track all of read
		/// operations on the _fileId
		//--------------------------------------------
		std::deque<IoMark> _readMarks;

		//--------------------------------------------
		/// Write deque that keeps track all of written
		/// operations on the _fileId
		//--------------------------------------------
		std::deque<IoMark> _writeMarks;


		//--------------------------------------------
		/// Deleted default constructor
		//--------------------------------------------
		IoStat() = delete;

	public:
		//--------------------------------------------
		/// Orthodoxe canonical form
		//--------------------------------------------
	
		//--------------------------------------------
		/// Destructor
		//--------------------------------------------
		~IoStat();

		//--------------------------------------------
		/// Constructor by copy constructor
		//--------------------------------------------
		IoStat(const IoStat &other);

		//--------------------------------------------
		/// Overload the operator =
		//--------------------------------------------
		IoStat& operator=(const IoStat &other);

		//--------------------------------------------
		/// @brief Main constructor to initialize the class
		///
		/// @param	fileId	Tracked file id
		/// @param	app		Name of the application where
		/// the file is linked
		/// @param	uid		Id of the user
		/// @param	gid		Id of the group
		//--------------------------------------------
		IoStat(uint64_t fileId, std::string app, uid_t uid, gid_t gid);

		//--------------------------------------------
		/// @brief Add bytes to the corresponding Read/Write deque
		/// and records its execution timestamp
		///
		/// @param	rBytes Number of bytes read
		/// @param	enumMark READ or WRITE variable comes
		/// from the IoStat::Marks enumerator
		//--------------------------------------------
		void add(size_t bytes, IoStat::Marks enumMark);

		//--------------------------------------------
		/// @brief Keep all I/O from the last N seconds.
		///
		/// @details
		/// The function takes as parameters the number of
		/// seconds corresponding to the number of seconds
		/// of I/O which will be kept, the rest will be erased.
		/// 
		/// @param	enumMark READ or WRITE variable comes
		/// from the IoStat::Marks enumerator
		/// @param seconds(optional) The number of last seconds of
		/// I/O that will be kept (by default - 10s)
		///
		/// @return -1 If the enumerator is incorrect
		/// @return 1 If there is nothing to delete
		/// @return size Else returns the size of what was erased
		//--------------------------------------------
		uint64_t cleanOldsMarks(Marks enumMark, size_t seconds = 10);


		//--------------------------------------------
		/// @brief Calculate the write or read bandwidth
		///
		/// @details
		/// The function calculates the read or
		/// write bandwidth (depending on the "enumMark" parameter)
		/// of the last N seconds (by default - 10s)
		///
		/// The function thus calculates the average and
		/// the standard deviation of the range of data found
		/// 
		/// @param	enumMark READ or WRITE variable comes
		/// from the IoStat::Marks enumerator
		/// @param	range Sets the "range" variable to the
		/// number of elements that were found
		/// (can be set to NULL in which case the parameter
		/// is ignored)
		/// @param	seconds(optional) over how many seconds
		/// from now the function should calculate
		///
		/// @return std::pair<double, double> fisrt is the
		/// average, the second is the standard deviation 
		//--------------------------------------------
		std::pair<double, double> bandWidth(Marks EnumMark, size_t *range = NULL, size_t seconds = 10) const;

		//--------------------------------------------
		/// @brief Calculate the write or read IOPS
		///
		/// @details
		/// The function calculates the read or
		/// write IOPS (depending on the "enumMark" parameter)
		/// of the last N seconds (by default - 10s)
		///
		/// @param	enumMark READ or WRITE variable comes
		/// from the IoStat::Marks enumerator
		/// @param seconds(optional) over how many seconds
		/// from now the function should calculate
		///
		/// @return -1 If the enumerator is incorrect
		/// @return double The IOPS
		//--------------------------------------------
		double getIOPS(Marks enumMark, size_t seconds = 10) const;

		//--------------------------------------------
		/// Static function
		/// @brief Displays the string given as a parameter
		/// in a format corresponding to the class with the
		/// current timestamp
		///
		/// @param	os The output stream
		/// @param	msg The message to display
		//--------------------------------------------
		static void	printInfo(std::ostream &os, const char *msg);

		//--------------------------------------------
		/// Static function
		/// @brief Displays the string given as a parameter
		/// in a format corresponding to the class with the
		/// current timestamp
		///
		/// @param	os The output stream
		/// @param	msg The message to display
		//--------------------------------------------
		static void	printInfo(std::ostream &os, const std::string &msg);

		//--------------------------------------------
		/// Get current uid
		//--------------------------------------------
		uid_t getUid() const;

		//--------------------------------------------
		/// Get current gid
		//--------------------------------------------
		gid_t getGid() const;

		//--------------------------------------------
		/// Get current app name
		//--------------------------------------------
		std::string getApp() const;

		//--------------------------------------------
		/// @brief Get the size of corresponding
		/// READ or WRITE deck
		///
		/// @param	enumMark READ or WRITE variable comes
		/// from the IoStat::Marks enumerator
		//--------------------------------------------
		ssize_t getSize(Marks enumMark) const;
};


//--------------------------------------------
/// @brief Overload operator << to print the average
/// and standard deviation of the last 10 seconds
//--------------------------------------------
std::ostream& operator<<(std::ostream &os, const IoStat &other);

//--------------------------------------------
/// @brief Overload operator << to print
/// the entire multimap
//--------------------------------------------
std::ostream& operator<<(std::ostream &os, const std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> > &other);

//--------------------------------------------
/// @brief Overload operator << to print
/// a IoStatSummary object from a std::optional
//--------------------------------------------
std::ostream& operator<<(std::ostream &os, const std::optional<IoStatSummary> &opt);

//--------------------------------------------
/// @brief Overload operator << to print
/// a IoStatSummary object
//--------------------------------------------
std::ostream& operator<<(std::ostream &os, const IoStatSummary &opt);

//--------------------------------------------
/// @brief Overload operator << to print
/// a IoBuffer::Summary object
//--------------------------------------------
std::ostream& operator<<(std::ostream &os, const IoBuffer::Summary &sum);
