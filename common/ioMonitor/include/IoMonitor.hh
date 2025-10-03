//  File: IoMonitor.hh
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

#include <chrono>
#include <deque>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <mutex>
#include <optional>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <cstdint>
#include <sys/types.h>
#include <cmath>
#include <iostream>
#include <set>
#include <ctime>
#include <string.h>
#include <map>
#include <unistd.h>
#include <iomanip>
#include <numeric>
#include <assert.h>
#include <typeinfo>
#include "color.hh"
#include "../proto/IoBuffer.pb.h"

//--------------------------------------------
/// Main structure stored in IoStat
/// Keep the time of class creation
//--------------------------------------------
struct IoMark {
	struct timespec io_time;
    size_t bytes;

	//--------------------------------------------
	/// Main contructor
	//--------------------------------------------
    IoMark(size_t bytes) : bytes(bytes){
		clock_gettime(CLOCK_REALTIME, &io_time);
	}

	//--------------------------------------------
	/// Default contructor
	//--------------------------------------------
    IoMark() : bytes(0){
		clock_gettime(CLOCK_REALTIME, &io_time);
	}
};


//--------------------------------------------
/// @brief Get the current time
///
/// @return const char* the current time
//--------------------------------------------
const char*	getCurrentTime();


//--------------------------------------------
/// Namespace
///
/// If debug is "true" all debug phases of
/// the corresponding class will be displayed
//--------------------------------------------
namespace io {
	constexpr bool IoStatDebug = false;
	constexpr bool IoMapDebug = false;
	constexpr bool IoMarkDebug = false;
	constexpr bool IoAggregateMapDebug = false;
	constexpr bool IoAggregateDebug = false;

	//--------------------------------------------
	/// Enumerator that allows to keep the context
	/// of UID or GID type of a variable
	//--------------------------------------------
	enum class TYPE {
		UID,
		GID
	};
}

//--------------------------------------------
/// Summary of a IoStat bandwidth
//--------------------------------------------
struct IoStatSummary {
	//--------------------------------------------
	/// Read/Write bandwidth
	//--------------------------------------------
	std::optional<std::pair<double, double> > readBandwidth;
	std::optional<std::pair<double, double> > writeBandwidth;

	//--------------------------------------------
	/// Size of the read and write bandwidth
	//--------------------------------------------
	size_t rSize;
	size_t wSize;

	//--------------------------------------------
	/// read and write IOPS
	//--------------------------------------------
	double rIops;
	double wIops;

	//--------------------------------------------
	// Keep the time of class creation
	//--------------------------------------------
	struct timespec io_time;

	//--------------------------------------------
	// Keep the time of class creation
	//--------------------------------------------
	size_t winTime;

	//--------------------------------------------
	/// Default constructor to initialize the class
	//--------------------------------------------
	IoStatSummary() :
		readBandwidth(std::pair<double, double>(0,0)),
		writeBandwidth(std::pair<double, double>(0, 0)),
		rSize(0), wSize(0), rIops(0), wIops(0), winTime(0){
			clock_gettime(CLOCK_REALTIME, &io_time);
		}

	IoStatSummary(const IoBuffer::Summary &sum) :
		readBandwidth({sum.ravrg(), sum.rstd()}),
		writeBandwidth({sum.wavrg(), sum.wstd()}),
		rSize(sum.rsize()), wSize(sum.wsize()), rIops(sum.riops()), wIops(sum.wiops()),
		winTime(sum.wintime()){
			clock_gettime(CLOCK_REALTIME, &io_time);
		}

	IoBuffer::Summary& Serialize(IoBuffer::Summary &sum){
		if (readBandwidth.has_value()){
			sum.set_ravrg(readBandwidth->first);
			sum.set_rstd(readBandwidth->second);
		}
		if (writeBandwidth.has_value()){
			sum.set_wavrg(writeBandwidth->first);
			sum.set_wstd(writeBandwidth->second);
		}
		sum.set_rsize(rSize);
		sum.set_wsize(wSize);
		sum.set_riops(rIops);
		sum.set_wiops(wIops);
		sum.set_wintime(winTime);

		return sum;
	};

	IoStatSummary* Deserialize(const IoBuffer::Summary &sum){
		if (readBandwidth.has_value()){
			readBandwidth->first = sum.ravrg();
			readBandwidth->second = sum.rstd();
		}
		if (writeBandwidth.has_value()){
			writeBandwidth->first = sum.wavrg();
			writeBandwidth->second = sum.wstd();
		}
		rSize = sum.rsize();
		wSize = sum.wsize();
		rIops = sum.riops();
		wIops = sum.wiops();

		return this;
	};
};
