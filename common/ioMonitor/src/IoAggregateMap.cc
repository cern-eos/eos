//  File: IoAggregateMap.cc
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

#include "../include/IoAggregateMap.hh"

//--------------------------------------------
/// Main constructor
//--------------------------------------------
IoAggregateMap::IoAggregateMap() : _running(true){
	_thread = std::thread(&IoAggregateMap::updateAggregateLoop, this);
}

//--------------------------------------------
/// Optional constructor to not launch the
/// multithreading function
//--------------------------------------------
IoAggregateMap::IoAggregateMap(int) : _running(false){
}

//--------------------------------------------
/// Constructor by copy constructor
//--------------------------------------------
IoAggregateMap::IoAggregateMap(const IoAggregateMap &other){
	std::lock_guard<std::mutex> lock(other._mutex);
	_map = other._map;
	for (const auto &it: other._aggregates)
		_aggregates.emplace(it.first, std::make_unique<IoAggregate>(*it.second.get()));
	_running.store(other._running.load());
	if (_running.load())
		_thread = std::thread(&IoAggregateMap::updateAggregateLoop, this);
}

//--------------------------------------------
/// Overload the operator =
//--------------------------------------------
IoAggregateMap& IoAggregateMap::operator=(const IoAggregateMap &other){
	if (this != &other){
		{
			std::scoped_lock lock(_mutex, other._mutex);
			_map = other._map;
			_aggregates.clear();
			for (const auto &it: other._aggregates)
				_aggregates.emplace(it.first, std::make_unique<IoAggregate>(*it.second.get()));
			_running.store(other._running.load());
		}
		if (!_running.load()){
			std::unique_lock<std::mutex> lock(_mutex);
			if (io::IoAggregateMapDebug)
				assert(lock.owns_lock());
			_cv.notify_one();
		if (_thread.joinable())
			_thread.join();
		}
	}

	return *this;
}

//--------------------------------------------
/// Overload the operator []
//--------------------------------------------
std::unique_ptr<IoAggregate>& IoAggregateMap::operator[](size_t winTime){
	return (_aggregates[winTime]);
}

//--------------------------------------------
/// Destructor
//--------------------------------------------
IoAggregateMap::~IoAggregateMap(){
	if (_running.load()){
			std::unique_lock<std::mutex> lock(_mutex);
			if (io::IoAggregateMapDebug)
				assert(lock.owns_lock());
			_running.store(false);
			_cv.notify_one();
	}
	if (_thread.joinable())
		_thread.join();
}


//--------------------------------------------
/// Returns a vector of all available windows
//--------------------------------------------
std::optional<std::vector<size_t> > IoAggregateMap::getAvailableWindows() const{
	std::lock_guard<std::mutex> lock(_mutex);
	std::vector<size_t> windo;

	if (_aggregates.empty())
		return (std::nullopt);

	for (auto &it : _aggregates)
		windo.emplace_back(it.first);
	return windo;
}

//--------------------------------------------
/// @brief Adds an IoStat object to the map
/// with the corresponding elements
//--------------------------------------------
void IoAggregateMap::addRead(uint64_t inode, const std::string &app, uid_t uid, gid_t gid, size_t rbytes){
	std::lock_guard<std::mutex> lock(_mutex);
	_map.addRead(inode, app, uid, gid, rbytes);
}

//--------------------------------------------
/// @brief Adds an IoStat object to the map
/// with the corresponding elements
//--------------------------------------------
void IoAggregateMap::addWrite(uint64_t inode, const std::string &app, uid_t uid, gid_t gid, size_t wbytes){
	std::lock_guard<std::mutex> lock(_mutex);
	_map.addWrite(inode, app, uid, gid, wbytes);
}

//--------------------------------------------
/// Multithreaded function update all
/// IoAggregate object in _aggregates variable
/// every N seconds (1s by default)
//--------------------------------------------
void IoAggregateMap::updateAggregateLoop(){
	auto next_tick = std::chrono::steady_clock::now() + std::chrono::seconds(TIME_TO_UPDATE);

	while (_running.load()){
		std::unique_lock<std::mutex> lock(_mutex);
		_cv.wait_until(lock, next_tick, [this]{ return !_running;});
		if (!_running.load())
			break;
		if constexpr (io::IoAggregateMapDebug)
			printInfo(std::cerr, "tick");
		for (auto &maps : _aggregates)
			maps.second->update(_map);
		next_tick += std::chrono::seconds(TIME_TO_UPDATE);
	}
}

//--------------------------------------------
/// Add a new window time to the aggregatation
//--------------------------------------------
int IoAggregateMap::addWindow(size_t winTime){
	std::lock_guard<std::mutex> lock(this->_mutex);
	if (winTime < 10){
		if constexpr (io::IoAggregateMapDebug)
			printInfo(std::cout, "add window failed: " + std::to_string(winTime));
		return -1;
	}
	_aggregates.emplace(winTime, std::make_unique<IoAggregate>(winTime));
	if constexpr (io::IoAggregateMapDebug)
		printInfo(std::cout, "add window succeeded: " + std::to_string(winTime));
	return 0;
}

//--------------------------------------------
/// Get available apps
//--------------------------------------------
std::vector<std::string> IoAggregateMap::getApps(size_t winTime) const{
	std::lock_guard<std::mutex> lock(_mutex);
	if (_aggregates.find(winTime) == _aggregates.end())
		return (std::vector<std::string>());
	return (_aggregates.at(winTime)->getApps());
}

//--------------------------------------------
/// Get available uids
//--------------------------------------------
std::vector<uid_t> IoAggregateMap::getUids(size_t winTime) const{
	std::lock_guard<std::mutex> lock(_mutex);
	if (_aggregates.find(winTime) == _aggregates.end())
		return (std::vector<uid_t>());
	return (_aggregates.at(winTime)->getUids());
}

//--------------------------------------------
/// Get available gids
//--------------------------------------------
std::vector<gid_t> IoAggregateMap::getGids(size_t winTime) const{
	std::lock_guard<std::mutex> lock(_mutex);
	if (_aggregates.find(winTime) == _aggregates.end())
		return (std::vector<gid_t>());
	return (_aggregates.at(winTime)->getGids());
}

//--------------------------------------------
/// Return true if the window exists,
/// otherwise returns false
//--------------------------------------------
bool IoAggregateMap::containe(size_t winTime) const {
	std::lock_guard<std::mutex> lock(_mutex);
	return (_aggregates.find(winTime) != _aggregates.end());
}

//--------------------------------------------
/// Return true if the thack exists in the window,
/// otherwise returns false
//--------------------------------------------
bool IoAggregateMap::containe(size_t winTime, std::string appName) const {
	std::lock_guard<std::mutex> lock(_mutex);
	if (_aggregates.find(winTime) == _aggregates.end())
		return false;
	auto apps(_aggregates.at(winTime)->getApps());
	return (std::find(apps.cbegin(), apps.cend(), appName) != apps.end());
}

//--------------------------------------------
/// Return true if the thack exists in the window,
/// otherwise returns false
//--------------------------------------------
bool IoAggregateMap::containe(size_t winTime, io::TYPE type, size_t id) const {
	std::lock_guard<std::mutex> lock(_mutex);
	if ((_aggregates.find(winTime) == _aggregates.end())
		|| (type != io::TYPE::UID && type != io::TYPE::GID))
		return false;
	auto ids(type == io::TYPE::UID ? _aggregates.at(winTime)->getUids()
										: _aggregates.at(winTime)->getGids());
	return (std::find(ids.cbegin(), ids.cend(), id) != ids.end());
}

//--------------------------------------------
/// Return true if the window has been deleted,
/// otherwise returns false
//--------------------------------------------
bool IoAggregateMap::rm(size_t winTime){
	std::lock_guard<std::mutex> lock(_mutex);
	return _aggregates.erase(winTime);
}

//--------------------------------------------
/// Return true if the appName of the window 
/// has been deleted, otherwise returns false
//--------------------------------------------
bool IoAggregateMap::rm(size_t winTime, std::string &appName){
	std::lock_guard<std::mutex> lock(_mutex);
	if (_aggregates.find(winTime) == _aggregates.end())
		return false;

	_map.rm(appName);
	return (_aggregates.at(winTime)->rm(appName));
}

//--------------------------------------------
/// Return true if the uid/gid of the window 
/// has been deleted, otherwise returns false
//--------------------------------------------
bool IoAggregateMap::rm(size_t winTime, io::TYPE type, size_t id){
	std::lock_guard<std::mutex> lock(_mutex);
	if (_aggregates.find(winTime) == _aggregates.end())
		return false;

	if (type != io::TYPE::UID && type != io::TYPE::GID)
		return false;

	_map.rm(type, id);
	return (_aggregates.at(winTime)->rm(type, id));
}

//--------------------------------------------
/// Returns a reference to the IoMap object
//--------------------------------------------
const IoMap& IoAggregateMap::getIoMap() const{
	std::lock_guard<std::mutex> lock(_mutex);
	return _map;
}

//--------------------------------------------
/// Returns a iterator that points to the
/// first element in the %IoMap.
//--------------------------------------------
std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> >::iterator IoAggregateMap::begin(){
	std::lock_guard<std::mutex> lock(_mutex);
	return _map.begin();
}

//--------------------------------------------
/// Returns a iterator that points to the
/// last element in the %IoMap.
//--------------------------------------------
std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> >::iterator IoAggregateMap::end(){
	std::lock_guard<std::mutex> lock(_mutex);
	return _map.end();
}

//--------------------------------------------
/// Add a Bin object to the window's
/// IoAggregate, and set the window's index
/// to that Bin
//--------------------------------------------
int IoAggregateMap::shiftWindow(size_t winTime){
	std::lock_guard<std::mutex> lock(_mutex);
	if constexpr (io::IoAggregateMapDebug)
		printInfo(std::cout, "shiftWindow");
	if (_aggregates.find(winTime) == _aggregates.end())
		return -1;
	return _aggregates[winTime]->shiftWindow();
}

//--------------------------------------------
/// Changes the position of the index in
/// the IoAggregate object  of the specified window
//--------------------------------------------
int IoAggregateMap::shiftWindow(size_t winTime, size_t index){
	std::lock_guard<std::mutex> lock(_mutex);
	if constexpr (io::IoAggregateMapDebug)
		printInfo(std::cout, "shiftWindow");
	if (_aggregates.find(winTime) == _aggregates.end())
		return -1;
	return _aggregates[winTime]->shiftWindow(index);
}

//--------------------------------------------
/// Overload operator << to print
/// all window and IoAggregate object from
/// the %unordored_map _aggregation
//--------------------------------------------
std::ostream& operator<<(std::ostream &os, const IoAggregateMap &other){
	std::lock_guard<std::mutex> lock(other._mutex);
	os << C_GREEN << "[" << C_CYAN << "IoAggregateMap" << C_GREEN << "]" << C_RESET;
	os << C_GREEN << "[" << C_CYAN << "available window: " << other._aggregates.size() << C_GREEN << "]" << C_RESET << std::endl;
	if (other._aggregates.size() == 0)
		os << C_CYAN << "empty" << C_RESET << std::endl;
	for (auto &it : other._aggregates){
		os << C_GREEN << "[" << C_CYAN << "Window: " << it.first << C_GREEN << "]" << C_RESET;
		os << *it.second.get() << std::endl;
	}
	return os;
}

//--------------------------------------------
/// Display the string given as parameter in
/// specific format with the current time
//--------------------------------------------
void	IoAggregateMap::printInfo(std::ostream &os, const char *msg) const{
	const char *time = getCurrentTime();
	os << IOAGGREGATEMAP_NAME << " [" << time << "]: " << msg << std::endl;
}

//--------------------------------------------
/// Display the string given as parameter in
/// specific format with the current time
//--------------------------------------------
void	IoAggregateMap::printInfo(std::ostream &os, const std::string &msg) const{
	const char *time = getCurrentTime();
	os << IOAGGREGATEMAP_NAME << " [" << time << "]: " << msg << std::endl;
}
