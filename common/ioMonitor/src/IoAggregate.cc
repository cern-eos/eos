//  File: IoAggregate.cc
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

#include "../include/IoAggregate.hh"

//--------------------------------------------
/// Constructor by copy constructor
//--------------------------------------------
IoAggregate::IoAggregate(const IoAggregate &other){
	std::lock_guard<std::mutex> otherLock(other._mutex);
	_intervalSec = other._intervalSec;
	_currentIndex = other._currentIndex;
	_bins = other._bins;
	_currentTime = other._currentTime;
	_apps = other._apps;
	_uids = other._uids;
	_gids = other._gids;
}

//--------------------------------------------
/// Overload the operator =
//--------------------------------------------
IoAggregate& IoAggregate::operator=(const IoAggregate &other){
	if (this != &other){
		std::scoped_lock lock(_mutex, other._mutex);
		_intervalSec = other._intervalSec;
		_currentIndex = other._currentIndex;
		_bins = other._bins;
		_currentTime = other._currentTime;
		_apps = other._apps;
		_uids = other._uids;
		_gids = other._gids;
	}
	return *this;
}

//--------------------------------------------
/// Main constructor
//--------------------------------------------
IoAggregate::IoAggregate(size_t winTime)
	: _intervalSec(10), _currentIndex(0) ,_currentTime(std::chrono::system_clock::now()){
	if (winTime < 10)
		winTime = 10;
	if (winTime % _intervalSec != 0)
		winTime -= winTime % _intervalSec;
	_winTime = winTime;
	_bins.emplace_back(Bin());
}

//--------------------------------------------
/// Destructor
//--------------------------------------------
IoAggregate::~IoAggregate(){}

//--------------------------------------------
/// Updates the current bin according to the
/// appName/uid/gid which are tracked every N seconds
/// (depending on this->_intervalSec)
//--------------------------------------------
void IoAggregate::update(const IoMap &maps){
	auto diff = std::chrono::system_clock::now() - _currentTime;

	/// If time to update
	if (diff >= std::chrono::seconds(_intervalSec)){
		if constexpr (io::IoAggregateDebug)
			printInfo(std::cout, "updating window " + std::to_string(_winTime));
		/// for each object in app/uid/gid, get and add summary
	
		for (auto it = _apps.begin(); it != _apps.end(); it++){
			auto summary = maps.getSummary(*it, _intervalSec);
			if (!summary.has_value())
				summary.emplace(IoStatSummary());
			addSample(*it, summary.value());
		}

		for (auto it = _uids.begin(); it != _uids.end(); it++){
			auto summary = maps.getSummary(io::TYPE::UID, *it, _intervalSec);
			if (!summary.has_value())
				summary.emplace(IoStatSummary());
			addSample(io::TYPE::UID, *it, summary.value());
		}

		for (auto it = _gids.begin(); it != _gids.end(); it++){
			auto summary = maps.getSummary(io::TYPE::GID, *it, _intervalSec);
			if (!summary.has_value())
				summary.emplace(IoStatSummary());
			addSample(io::TYPE::GID, *it, summary.value());
		}
		_currentTime = std::chrono::system_clock::now();
	}
}

//--------------------------------------------
/// Add a Bin object, and set the index
/// to that Bin
//--------------------------------------------
int IoAggregate::shiftWindow(){
	_bins.emplace_back(Bin());
	_currentIndex = _bins.size() - 1;
	if constexpr (io::IoAggregateDebug)
		printInfo(std::cout, "shift Window succeeded");
	return _currentIndex;
}

//--------------------------------------------
/// Changes the position of the current
/// index (_currentIndex)
//--------------------------------------------
int IoAggregate::shiftWindow(const size_t index){
	if (index >= _bins.size())
		return -1;
	_currentIndex = index;
	if constexpr (io::IoAggregateDebug)
		printInfo(std::cout, "shift Window succeeded");
	return _currentIndex;
}

//--------------------------------------------
/// Condenses a vector of IoStatSummary
/// into a single one
//--------------------------------------------
std::optional<IoStatSummary> IoAggregate::summaryWeighted(const std::vector<IoStatSummary> &summarys, size_t winTime){
	size_t rDivisor = 0;
	size_t wDivisor = 0;
	IoStatSummary weighted;

	if (io::IoAggregateDebug)
		printInfo(std::cout, "summary weighted called");

	/// Calcule average and IOPS
	for (const auto &it : summarys){
		if (it.readBandwidth.has_value()){
			weighted.readBandwidth->first += (it.readBandwidth->first * it.rSize);
			weighted.rIops += it.rIops * it.rSize;
		}
		if (it.writeBandwidth.has_value()){
			weighted.writeBandwidth->first += (it.writeBandwidth->first * it.wSize);
			weighted.wIops += it.wIops * it.wSize;
		}
		rDivisor += it.rSize;
		wDivisor += it.wSize;
	}

	if (rDivisor > 0){
		weighted.readBandwidth->first /= rDivisor;
		weighted.rIops /= rDivisor;
	}
	if (wDivisor > 0){
		weighted.writeBandwidth->first /= wDivisor;
		weighted.wIops /= wDivisor;
	}

	/// Calcule standard deviation
	for (const auto &it : summarys){
		if (weighted.readBandwidth.has_value())
			weighted.readBandwidth->second += (it.rSize * \
				(std::pow(it.readBandwidth->second, 2) + std::pow(it.readBandwidth->first - \
													  weighted.readBandwidth->first, 2)));
		if (weighted.writeBandwidth.has_value())
			weighted.writeBandwidth->second += (it.wSize * \
				(std::pow(it.writeBandwidth->second, 2) + std::pow(it.writeBandwidth->first - \
													  weighted.writeBandwidth->first, 2)));
	}

	if (rDivisor > 0 && weighted.readBandwidth.has_value())
			weighted.readBandwidth->second = std::sqrt(weighted.readBandwidth->second / rDivisor);

	if (wDivisor > 0 && weighted.writeBandwidth.has_value())
			weighted.writeBandwidth->second = std::sqrt(weighted.writeBandwidth->second / wDivisor);


	weighted.rSize = rDivisor;
	weighted.wSize = wDivisor;
	
	/// Check empty case
	if (io::IoAggregateDebug)
		printInfo(std::cout, "summary weighted succeeded");
	if (weighted.wSize == 0 && weighted.rSize == 0)
		return std::nullopt;
	if (weighted.rSize == 0)
		weighted.readBandwidth = std::nullopt;
	if (weighted.wSize == 0)
		weighted.writeBandwidth = std::nullopt;

	weighted.winTime = winTime;
	return weighted;
}

//--------------------------------------------
/// Get available apps
//--------------------------------------------
std::vector<std::string> IoAggregate::getApps() const{
	return (std::vector(_apps.begin(), _apps.end()));
}

//--------------------------------------------
/// IoAggregateMap::Get available uids
//--------------------------------------------
std::vector<uid_t> IoAggregate::getUids() const{
	return (std::vector(_uids.begin(), _uids.end()));
}

//--------------------------------------------
/// IoAggregateMap::Get available gids
//--------------------------------------------
std::vector<gid_t> IoAggregate::getGids() const{
	return (std::vector(_gids.begin(), _gids.end()));
}

//--------------------------------------------
/// Get current index
//--------------------------------------------
size_t IoAggregate::getIndex() const{ return _currentIndex;}

//--------------------------------------------
/// Display the string given as parameter in
/// specific format with the current time
//--------------------------------------------
void	IoAggregate::printInfo(std::ostream &os, const char *msg){
	const char *time = getCurrentTime();
	os << IOAGGREGATE_NAME << " [" << time << "]: " << msg << std::endl;
}

//--------------------------------------------
/// Display the string given as parameter in
/// specific format with the current time
//--------------------------------------------
void	IoAggregate::printInfo(std::ostream &os, const std::string &msg){
	const char *time = getCurrentTime();
	os << IOAGGREGATE_NAME << " [" << time << "]: " << msg << std::endl;
}

//--------------------------------------------
/// Overload operator << to print
/// a IoAggregate object
//--------------------------------------------
std::ostream& operator<<(std::ostream &os, const IoAggregate &other){
	std::lock_guard<std::mutex> lock(other._mutex);
	os << C_GREEN << "[" << C_CYAN << "IoAggregate" << C_GREEN << "]" << C_RESET << std::endl;
	os << C_GREEN << "[" << C_YELLOW << "window time: " << other._winTime << C_GREEN << "]" << C_RESET;
	os << C_GREEN << "[" << C_YELLOW << "interval/win: " << other._intervalSec << C_GREEN << "]" << C_RESET;
	os << C_GREEN << "[" << C_YELLOW << "nbr of bin: " << other._bins.size() << C_GREEN << "]" << C_RESET;
	os << C_GREEN << "[" << C_YELLOW << "currentIndex: " << other._currentIndex << C_GREEN << "]" << C_RESET << std::endl;
	
	os << C_BLUE;
	os << "\t[Tracks]" << std::endl;
	os << "\t apps:" << std::endl;
	for (auto it : other._apps)
		os << "\t  - " << it << std::endl;
	os << "\t uids:" << std::endl;
	for (auto it : other._uids)
		os << "\t  - " << it << std::endl;
	os << "\t gids:" << std::endl;
	for (auto it : other._gids)
		os << "\t  - " << it << std::endl;

	auto it = other._bins.at(other._currentIndex);
	if (it.appStats.size() > 0){
		os << "apps: [" << it.appStats.size() << "]" << std::endl;
		for (auto apps : it.appStats)
			os << "\t[" << apps.first << "]" << std::endl << "\t- " << apps.second << std::endl;
	}
	if (it.uidStats.size() > 0){
		os << "uids: [" << it.uidStats.size() << "]" << std::endl;
		for (auto uids : it.uidStats)
			os << "\t[" << uids.first << "]" << std::endl << "\t- " << uids.second << std::endl;
	}
	if (it.gidStats.size() > 0){
		os << "gids: [" << it.gidStats.size() << "]" << std::endl;
		for (auto gids : it.gidStats)
			os << "\t[" << gids.first << "]" << std::endl << "\t- " << gids.second << std::endl;
	}

	os << C_RESET;
	return os;
}
