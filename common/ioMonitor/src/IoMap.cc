//  File: IoMap.cc
//  Author: Ilkay Yanar - 42Lausanne /CERN
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

#include "../include/IoMap.hh"

//--------------------------------------------
/// Public static mutex to share outputs stream
//--------------------------------------------
std::mutex IoMap::_osMutex;

//--------------------------------------------
/// Main constructor
//--------------------------------------------
IoMap::IoMap() : _running(true){
	_cleaner = std::thread(&IoMap::cleanerLoop, this);
}

//--------------------------------------------
/// Constructor by copy constructor
//--------------------------------------------
IoMap::IoMap(const IoMap &other){
	std::lock_guard<std::mutex> lock(other._mutex);
	_filesMap = other._filesMap;
	_apps = other._apps;
	_uids = other._uids;
	_gids = other._gids;
	_running = other._running.load();
	if (_running.load())
		_cleaner = std::thread(&IoMap::cleanerLoop, this);
}

//--------------------------------------------
/// Overload the operator =
//--------------------------------------------
IoMap& IoMap::operator=(const IoMap &other){
	if (this != &other){
		{
			std::scoped_lock lock(_mutex, other._mutex);
			_filesMap = other._filesMap;
			_apps = other._apps;
			_uids = other._uids;
			_gids = other._gids;
			_running.store(other._running.load());
		}
		{
			if (!_running.load()){
				std::unique_lock<std::mutex> lock(_mutex);
				if (io::IoAggregateMapDebug)
					assert(lock.owns_lock());
				_cv.notify_one();
			if (_cleaner.joinable())
				_cleaner.join();
			}
		}
	}

	return *this;
}

//--------------------------------------------
/// Destructor
//--------------------------------------------
IoMap::~IoMap() {
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (io::IoMapDebug)
			assert(lock.owns_lock());
		_running.store(false);
		_cv.notify_one();
	}
	if (_cleaner.joinable())
		_cleaner.join();
}

//--------------------------------------------
/// Constructor for disable mutlithreading
//--------------------------------------------
IoMap::IoMap(int) : _running(false){}

//--------------------------------------------
/// Display the string given as parameter in
/// specific format with the current time
//--------------------------------------------
void	IoMap::printInfo(std::ostream &os, const char *msg) const{
	const char *time = getCurrentTime();
	os << IOMAP_NAME << " [" << time << "]: " << msg << std::endl;
}

//--------------------------------------------
/// Display the string given as parameter in
/// specific format with the current time
//--------------------------------------------
void	IoMap::printInfo(std::ostream &os, const std::string &msg) const{
	const char *time = getCurrentTime();
	os << IOMAP_NAME << " [" << time << "]: " << msg << std::endl;
}

//--------------------------------------------
/// Multithreaded function to clean up
/// inactive IoStats
//--------------------------------------------
void IoMap::cleanerLoop(){
	while (_running.load()){
		std::unique_lock<std::mutex> lock(_mutex);

		_cv.wait_for(lock, std::chrono::seconds(TIME_TO_CLEAN), [this]{ return !_running;});
		if (!_running.load())
			break;

		// Clean inactive I/O
		size_t rsize = 0;
		size_t wsize = 0;

		for(auto it = _filesMap.begin(); it != _filesMap.end();){
			std::pair<double, double> read;
			std::pair<double, double> write;
			read = it->second->bandWidth(IoStat::Marks::READ, &rsize, TIME_TO_CLEAN);
			write = it->second->bandWidth(IoStat::Marks::WRITE, &wsize, TIME_TO_CLEAN);
			if ((read.first == 0 && read.second == 0)
				&& (write.first == 0 && write.second == 0)
				&& (rsize == 0 && wsize == 0))
				it = _filesMap.erase(it);
			else
				it++;
			rsize = 0;
			wsize = 0;
		}
	}
}

//--------------------------------------------
/// Adds an IoStat object to the multimap with
/// the corresponding elements
//--------------------------------------------
void IoMap::addRead(uint64_t inode, const std::string &app, uid_t uid, gid_t gid, size_t rbytes){
	std::lock_guard<std::mutex> lock(_mutex);

	auto it = _filesMap.equal_range(inode);
	// Create new IoStat or search for an existing matching one
	if (_filesMap.find(inode) == _filesMap.end()){
		if (io::IoMapDebug)
			printInfo(std::cout, "add new");
		auto newIo = _filesMap.insert({inode, std::make_shared<IoStat>(inode, app, uid, gid)});
		newIo->second->add(rbytes, IoStat::Marks::READ);
		_apps.insert(app);
		_uids.insert(uid);
		_gids.insert(gid);
		return ;
	}
	while (it.first != it.second){
		auto &io = it.first->second;
		if (io->getApp() == app && io->getGid() == gid && io->getUid() == uid){
			io->add(rbytes, IoStat::Marks::READ);
			if (io::IoMapDebug)
				printInfo(std::cout, "addRead");
			break ;
		}
		it.first++;
		if (it.first == it.second){
			if (io::IoMapDebug)
				printInfo(std::cout, "add new");
			auto newIo = _filesMap.insert({inode, std::make_shared<IoStat>(inode, app, uid, gid)});
			newIo->second->add(rbytes, IoStat::Marks::READ);
			_apps.insert(app);
			_uids.insert(uid);
			_gids.insert(gid);
			break ;
		}
	}
}

//--------------------------------------------
/// Adds an IoStat object to the multimap
/// with the corresponding elements
//--------------------------------------------
void IoMap::addWrite(uint64_t inode, const std::string &app, uid_t uid, gid_t gid, size_t wbytes){
	std::lock_guard<std::mutex> lock(_mutex);

	auto it = _filesMap.equal_range(inode);
	// Create new IoStat or search for an existing matching one
	if (_filesMap.find(inode) == _filesMap.end()){
		if (io::IoMapDebug)
			printInfo(std::cout, "add new");
		auto newIo = _filesMap.insert({inode, std::make_shared<IoStat>(inode, app, uid, gid)});
		newIo->second->add(wbytes, IoStat::Marks::WRITE);
		_apps.insert(app);
		_uids.insert(uid);
		_gids.insert(gid);
		return ;
	}
	while (it.first != it.second){
		auto &io = it.first->second;
		if (io->getApp() == app && io->getGid() == gid && io->getUid() == uid){
			io->add(wbytes, IoStat::Marks::WRITE);
			if (io::IoMapDebug)
				printInfo(std::cout, "addWrite");
			break ;
		}
		it.first++;
		if (it.first == it.second){
			if (io::IoMapDebug)
				printInfo(std::cout, "add new");
			auto newIo = _filesMap.insert({inode, std::make_shared<IoStat>(inode, app, uid, gid)});
			newIo->second->add(wbytes, IoStat::Marks::WRITE);
			_apps.insert(app);
			_uids.insert(uid);
			_gids.insert(gid);
			break ;
		}
	}
}

bool IoMap::rm(std::string &appName){
	std::lock_guard<std::mutex> lock(_mutex);
	if (_apps.find(appName) == _apps.end())
		return false;

	_apps.erase(appName);
	return true;
}

bool IoMap::rm(io::TYPE type, size_t id){
	std::lock_guard<std::mutex> lock(_mutex);
	if (type != io::TYPE::UID && type != io::TYPE::GID)
		return false;
	else if (type == io::TYPE::UID ? _uids.find(id) == _uids.end() : _gids.find(id) == _gids.end())
		return false;

	type == io::TYPE::UID ? _uids.erase(id) : _gids.erase(id);

	return true;
}


//--------------------------------------------
/// Get all apps
//--------------------------------------------
std::vector<std::string> IoMap::getApps() const{
	std::lock_guard<std::mutex> lock(_mutex);
	std::vector<std::string> appsName(_apps.begin(), _apps.end());
	return (appsName);
}

//--------------------------------------------
/// Get all uids
//--------------------------------------------
std::vector<uid_t> IoMap::getUids() const{
	std::lock_guard<std::mutex> lock(_mutex);
	std::vector<uid_t> uids(_uids.begin(), _uids.end());
	return (uids);
}

//--------------------------------------------
/// Get all gids
//--------------------------------------------
std::vector<gid_t> IoMap::getGids() const{
	std::lock_guard<std::mutex> lock(_mutex);
	std::vector<gid_t> gids(_gids.begin(), _gids.end());
	return (gids);
}

//--------------------------------------------
/// Get a copy of the multimap
//--------------------------------------------
std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> > IoMap::GetAllStatsSnapshot() const{
	std::lock_guard<std::mutex> lock(_mutex);
	return (_filesMap);
}

//--------------------------------------------
/// Overload operator << to print the entire
/// multimap from a IoMap object
//--------------------------------------------
std::ostream& operator<<(std::ostream &os, const IoMap &other){
	for (auto it : other._filesMap){
		os << C_GREEN << "┌─[" << C_CYAN << "IoMap" << C_GREEN << "]" << C_RESET;
		os << C_GREEN << "[" << C_CYAN << "id:" << it.first << C_GREEN << "]" << C_RESET;
		os << C_GREEN << "[" <<  C_CYAN << "app:"<< it.second->getApp() << C_GREEN << "]" << C_RESET;
		os << C_GREEN << "[" << C_CYAN << "uid:" << it.second->getUid() << C_GREEN << "]" << C_RESET;
		os << C_GREEN << "[" << C_CYAN << "gid:" << it.second->getGid() << C_GREEN << "]" << C_RESET;
		os << C_GREEN << "[" << C_CYAN << "sR:" << it.second->getSize(IoStat::Marks::READ)
			<< "/sW:"<< it.second->getSize(IoStat::Marks::WRITE) << C_GREEN << "]" << C_RESET;
		os << std::endl << C_GREEN << "└─[" << C_CYAN << "IoStat" << C_GREEN << "]" << C_RESET;
		os << C_WHITE << *it.second.get() << C_RESET << std::endl;
	}
	return os;
}

//--------------------------------------------
/// Returns a iterator that points to the first
/// element in the %unordered_multimap.
//--------------------------------------------
std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> >::iterator IoMap::begin(){return _filesMap.begin();}

//--------------------------------------------
/// Returns a iterator that points to the last
/// element in the %unordered_multimap.
//--------------------------------------------
std::unordered_multimap<uint64_t, std::shared_ptr<IoStat> >::iterator IoMap::end(){return _filesMap.end();}


//--------------------------------------------
/// Calculates the weighted average and
/// standard deviation
//--------------------------------------------
std::pair<double, double> IoMap::calculeWeighted(std::map<std::pair<double, double>, size_t> &indexData) const{
	size_t divisor = 0;
	std::pair<double, double> weighted = {0, 0};

	/// Calcule average
	for (const auto &it : indexData){
		weighted.first += (it.first.first * it.second);
		divisor += it.second;
	}
	if (divisor > 0)
		weighted.first /= divisor;

	/// Calcule Standard deviation
	for (const auto &it : indexData)
		weighted.second += it.second * (std::pow(it.first.second, 2) + std::pow(it.first.first - weighted.first, 2));

	if (divisor > 0)
		weighted.second = std::sqrt(weighted.second / divisor);

	return weighted;
}
