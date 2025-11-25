//  File: testIoMap.cc
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

#include "tester.hh"

void prompt(bool &isMultiT, std::string &input){
	while (true){
		if (isMultiT)
			std::cout << "[MultiThreading][IoMap]-> ";
		else
			std::cout << "[SingleThread][IoMap]-> ";
		std::getline(std::cin, input);
		if (input.empty())
			continue;
		break;
	}
}

void print(IoMap *map){
	auto snap = map->GetAllStatsSnapshot();
	for (auto it : snap){
		std::cout << C_GREEN << "┌─[" << C_CYAN << "Map" << C_GREEN << "]" << C_RESET;
		std::cout << C_GREEN << "[" << C_CYAN << "id:" << it.first << C_GREEN << "]" << C_RESET;
		std::cout << C_GREEN << "[" <<  C_CYAN << "app:"<< it.second->getApp() << C_GREEN << "]" << C_RESET;
		std::cout << C_GREEN << "[" << C_CYAN << "uid:" << it.second->getUid() << C_GREEN << "]" << C_RESET;
		std::cout << C_GREEN << "[" << C_CYAN << "gid:" << it.second->getGid() << C_GREEN << "]" << C_RESET;
		std::cout << C_GREEN << "[" << C_CYAN << "sR:" << it.second->getSize(IoStat::Marks::READ)
			<< "/sW:"<< it.second->getSize(IoStat::Marks::WRITE) << C_GREEN << "]" << C_RESET;
		std::cout << std::endl << C_GREEN << "└─[" << C_CYAN << "IoStat" << C_GREEN << "]" << C_RESET;
		std::cout << C_WHITE << *it.second << C_RESET << std::endl << std::endl;
	}

	std::cout << C_GREEN << "[" << C_CYAN << "apps" << C_GREEN << "] : " << C_CYAN;
	auto apps = map->getApps();
	for (auto it = apps.begin(); it != apps.end();){
		std::cout << *it;
		it++;
		if (it != apps.end())
			std::cout << ", ";
	}
	std::cout << C_RESET << std::endl << std::endl;

	std::cout << C_GREEN << "[" << C_CYAN << "uids" << C_GREEN << "] : " << C_CYAN;
	auto uids = map->getUids();
	for (auto it = uids.begin(); it != uids.end();){
		std::cout << *it;
		it++;
		if (it != uids.end())
			std::cout << ", ";
	}
	std::cout << C_RESET << std::endl << std::endl;

	std::cout << C_GREEN << "[" << C_CYAN << "gids" << C_GREEN << "] : " << C_CYAN;
	auto gids = map->getGids();
	for (auto it = gids.begin(); it != gids.end();){
		std::cout << *it;
		it++;
		if (it != gids.end())
			std::cout << ", ";
	}
	std::cout << C_RESET << std::endl;
}

void fill(IoMap &map, std::mutex &mutex){
	std::string input;

	while (true){
		std::cout << "fill with interaction? [y/n]: ";
		std::getline(std::cin, input);
		if (input.empty())
			continue;
		else if (input == "n"){
			fillData(map);
			std::cout << "fill data succeed" << std::endl;
		}
		else if (input == "y"){
			if (!fillDataInteract(map, mutex))
				std::cout << "fill data interact succeed" << std::endl;
			else
				std::cout << "fill data interact failed" << std::endl;
		}
		break;
	}
}

void purge(bool &isMultiT, IoMap *map){
	if (map){
		delete map;
		if (isMultiT)
			map = new IoMap();
		else
			map = new IoMap(0);
	}
}

static void printUsage(){
	std::cout << "Usage:" << std::endl;
	std::cout << "$ [command]" << std::endl;
	std::cout << std::endl;

	std::cout << "META OPTIONS" << std::endl;
	std::cout << "  h, help \tshow list of command-line options." << std::endl;
	std::cout << std::endl;

	std::cout << "COMMANDS" << std::endl;
	std::cout << "  p, \tprint \tprint the map" << std::endl;
	std::cout << "  fill, \tfill the map with I/O" << std::endl;
	std::cout << "  purge, \tclear the map" << std::endl;
	std::cout << "  c, \tclear\tclear the terminal" << std::endl;
	std::cout << "  exit, \texit monitor" << std::endl;
	std::cout << std::endl;
}

static int rm(IoMap *map, std::stringstream &os){
	std::string cmd;
	uid_t uid = 0;
	uid_t gid = 0;

	if (os >> cmd){
		if (cmd == "uid" && os >> uid)
			return map->rm(io::TYPE::UID, uid);
		if (cmd == "gid" && os >> gid)
			return map->rm(io::TYPE::GID, gid);
		else
			return map->rm(cmd);
	}
	return -1;
}

int execCmd(std::string &input, IoMap *map, bool &isMultiT, std::mutex &mutex){
	std::stringstream os(input);
	std::string cmd;
	os >> cmd;
	if (cmd == "exit"){
		std::cout << "exit" << std::endl;
		return 1;
	}
	else if (cmd == "print" || cmd == "p")
		print(map);
	else if (cmd == "fill"){
		if (os >> cmd)
			std::cerr << "IoMap: " << input << " :command not found" << std::endl;
		else
			fill(*map, mutex);
	}
	else if (cmd == "clear" || cmd == "c")
		std::cout << "\033c";
	else if (cmd == "purge")
		purge(isMultiT, map);
	else if (cmd == "h" || cmd == "help")
		printUsage();
	else if (cmd == "rm")
		std::cout << "rm: " << rm(map, os) << std::endl;
	else
		std::cerr << "IoMap: " << input << " :command not found" << std::endl;
	if (cmd != "clear" && cmd != "c")
		std::cout << std::endl;
	return 0;
}

int testInteractiveIoMap(){
	IoMap *map;
	bool isMultiT = false;
	std::mutex mutex;

	while (true){
		std::lock_guard<std::mutex> lock(IoMap::_osMutex);
		std::string tmp;
		std::cout << "run multithreading? [y/n]: ";
		std::getline(std::cin, tmp);
		if (tmp != "y" && tmp != "n")
			continue;
		tmp == "y" ? isMultiT = true : isMultiT = false;
		if (isMultiT)
			map = new IoMap();
		else
			map = new IoMap(0);
		std::cout << "\033c";
		break;
	}
	while(true){
		std::unique_lock<std::mutex> lock(IoMap::_osMutex);
		std::string input;
		if (isMultiT)
			lock.unlock();
		prompt(isMultiT, input);
		if (execCmd(input, map, isMultiT, mutex) == 1)
			break ;
	}
	delete map;
	return 0;
}

int testIoMapData(){
	std::multimap<std::string, std::optional<std::pair<double, double> > > data;
	std::vector<std::unique_ptr<IoMap> > maps;
	size_t nbrOfMap = 10;

	for (size_t i = 0; i < nbrOfMap; i++)
		maps.push_back(std::make_unique<IoMap>());
	for (size_t i = 0; i < nbrOfMap; i++){
		fillData(*maps.at(i).get());
	}

	for (size_t i = 0; i < 50; i++){
		std::lock_guard<std::mutex> lock(IoMap::_osMutex);

		for (auto &it : maps){
			data.insert({"mgm", it->getBandwidth("mgm", IoStat::Marks::READ, 2)});
			data.insert({"mgm", it->getBandwidth("mgm", IoStat::Marks::WRITE, 2)});
			data.insert({"uid_t: 2", it->getBandwidth(io::TYPE::UID, 2, IoStat::Marks::READ)});
			data.insert({"uid_t: 2", it->getBandwidth(io::TYPE::UID, 2, IoStat::Marks::WRITE)});
			data.insert({"gid_t: 1", it->getBandwidth(io::TYPE::GID, 1, IoStat::Marks::READ)});
			data.insert({"gid_t: 1", it->getBandwidth(io::TYPE::GID, 1, IoStat::Marks::WRITE)});
			if (io::IoMapDebug){
				for (auto &it : data){
					if (it.second.has_value()){
						std::cout << "map[" << it.first << "]: "
							<< "avrg: " << it.second->first << " | standard deviation: " << it.second->second << std::endl;
					}
					else
						std::cout << "no value" << std::endl;
				}
				std::cout << std::endl;
			}
		}
	}
	return 0;
}

/// Print std::pair
std::ostream& operator<<(std::ostream &os, const std::pair<double, double> &other){
	os << "[pair bandwidth] " << std::endl;
	os << C_BLUE << "{average: " << other.first <<
		", standard deviation: " << other.second <<  "}" << C_RESET << std::endl;
	return os;
}

/// Test if the average and standard deviation calcule are correct
int testIoMapSpecificCase(){
	IoMap map;

	map.addRead(1, "cernbox", 2, 1, 3531);
	map.addRead(1, "cernbox", 2, 1, 4562);
	map.addRead(1, "cernbox", 2, 1, 4573);
	map.addRead(1, "cernbox", 2, 1, 1332);
	map.addRead(1, "cernbox", 2, 1, 34563);
	map.addRead(1, "cernbox", 2, 1, 35);
	map.addRead(1, "cernbox", 2, 1, 544);

	std::optional<std::pair<double, double> > it = map.getBandwidth("cernbox", IoStat::Marks::READ);
	if (it.has_value()){
		if (static_cast<int>(it->first) != 7020 || static_cast<int>(it->second) != 11376)
			return -1;
	}
	else
		return -1;
	return 0;
}

void pairTmp(std::pair<double, double> *p1, std::pair<double, double> *p2, std::pair<double, double> *p3){
	p1->first = 42;
	p2->first = 65;
	p3->first = 56;
	double deviation = 0;
	deviation += std::pow(std::abs(50 - p1->first), 2);
	deviation += std::pow(std::abs(50 - p1->first), 2);
	deviation += std::pow(std::abs(26 - p1->first), 2);
	deviation = std::sqrt(deviation / 3);
	p1->second = deviation;
	deviation = 0;
	deviation += std::pow(std::abs(64 - p2->first), 2);
	deviation += std::pow(std::abs(97 - p2->first), 2);
	deviation += std::pow(std::abs(34 - p2->first), 2);
	deviation = std::sqrt(deviation / 3);
	p2->second = deviation;
	deviation = 0;
	deviation += std::pow(std::abs(97 - p3->first), 2);
	deviation += std::pow(std::abs(27 - p3->first), 2);
	deviation += std::pow(std::abs(44 - p3->first), 2);
	deviation = std::sqrt(deviation / 3);
	p3->second = deviation;
}


int testIoMapExactValue(){
	IoMap map;

	// Calculate raw data
	std::pair<double, double> p1;
	std::pair<double, double> p2;
	std::pair<double, double> p3;

	pairTmp(&p1, &p2, &p3);
	double avrg = 0;
	double deviation = 0;

	// weighted average
	avrg += p1.first * 3;
	avrg += p2.first * 3;
	avrg += p3.first * 3;
	avrg /= 9;

	// weighted standard deviation
	deviation += 3 * (std::pow(p1.second, 2) + std::pow(p1.first - avrg, 2));
	deviation += 3 * (std::pow(p2.second, 2) + std::pow(p2.first - avrg, 2));
	deviation += 3 * (std::pow(p3.second, 2) + std::pow(p3.first - avrg, 2));
	deviation = std::sqrt(deviation / 9);

	// Test IoMap function
	map.addWrite(1, "cernbox", 2, 1, 50);
	map.addWrite(1, "cernbox", 2, 1, 50);
	map.addWrite(1, "cernbox", 2, 1, 26);

	map.addWrite(1, "cernbox", 42, 42, 64);
	map.addWrite(1, "cernbox", 42, 42, 97);
	map.addWrite(1, "cernbox", 42, 42, 34);

	map.addWrite(1, "cernbox", 78, 5, 97);
	map.addWrite(1, "cernbox", 78, 5, 27);
	map.addWrite(1, "cernbox", 78, 5, 44);

	// if no data or difference between raw data and function return -1
	std::optional<std::pair<double, double> > it = map.getBandwidth("cernbox", IoStat::Marks::WRITE);
	if (it.has_value()){
		if (it->first != avrg || it->second != deviation)
			return -1;
	}
	else
		return -1;
	return 0;
}

int testIoMapBigVolume(){
	std::vector<std::optional<std::pair<double, double> > > data;
	std::vector<std::unique_ptr<IoMap> > maps;
	size_t nbrOfMap = 100;

	IoMark begin;
	for (size_t i = 0; i < nbrOfMap; i++)
		maps.push_back(std::make_unique<IoMap>());
	for (size_t i = 0; i < nbrOfMap; i++)
		fillData(*maps.at(i).get());

	IoMark end;
	long diff = TIME_TO_CLEAN * 2 + 1;
	std::this_thread::sleep_for(std::chrono::seconds(diff));
	data.clear();
	for (auto &it : maps){
		for (size_t i = 0; i < 1000; i++){
			data.push_back(it->getBandwidth("mgm", IoStat::Marks::READ, 30));
			data.push_back(it->getBandwidth("mgm", IoStat::Marks::WRITE, 30));
			data.push_back(it->getBandwidth("fdf", IoStat::Marks::READ, 30));
			data.push_back(it->getBandwidth("fdf", IoStat::Marks::WRITE, 30));
			data.push_back(it->getBandwidth("miniRT", IoStat::Marks::READ, 30));
			data.push_back(it->getBandwidth("miniRT", IoStat::Marks::WRITE, 30));
		}
	}
	for (auto &it : data){
		if (it.has_value())
			return -1;
	}
	return 0;
}

int testIoMapIds(){
	IoMap map;

	map.addRead(1, "eos", 40, 24, 564);
	map.addRead(1, "eos", 40, 24, 443);
	map.addRead(1, "eos", 40, 24, 554);
	map.addRead(1, "eos", 40, 24, 20);
	map.addRead(1, "eos", 40, 24, 4220);
	map.addRead(1, "eos", 40, 24, 24250);

	map.addRead(1, "eos", 42, 24, 125);
	map.addRead(1, "eos", 42, 24, 24);
	map.addRead(1, "eos", 42, 24, 24);
	map.addRead(1, "eos", 42, 24, 24);
	map.addRead(1, "eos", 42, 24, 48);

	map.addRead(4, "eos", 56, 44, 15);
	map.addRead(4, "eos", 56, 44, 142);
	map.addRead(4, "eos", 56, 44, 155);

	{
		auto it = map.getBandwidth("eos", IoStat::Marks::READ);
		if (it.has_value()){
			if (static_cast<int>(it->first) != 2186 || static_cast<int>(it->second) != 6209)
				return -1;
		} else
			return -1;
	}
	{
		auto it = map.getBandwidth(io::TYPE::GID, 24, IoStat::Marks::READ);
		if (it.has_value()){
			if (static_cast<int>(it->first) != 2754 || static_cast<int>(it->second) != 6897)
				return -1;
		} else
			return -1;
	}
	{
		auto it = map.getBandwidth(io::TYPE::UID, 24, IoStat::Marks::READ);
		if (it.has_value())
			return -1;
	}

	return 0;
}

int testIoMapSummary(){
	IoMap map;

	// Add Read
	map.addRead(1, "eos", 40, 24, 564);
	map.addRead(1, "eos", 40, 24, 443);
	map.addRead(1, "eos", 40, 24, 554);
	map.addRead(1, "eos", 40, 24, 20);
	map.addRead(1, "eos", 40, 24, 4220);
	map.addRead(1, "eos", 40, 24, 24250);

	map.addRead(1, "eos", 42, 24, 125);
	map.addRead(1, "eos", 42, 24, 24);
	map.addRead(1, "eos", 42, 24, 24);
	map.addRead(1, "eos", 42, 24, 24);
	map.addRead(1, "eos", 42, 24, 48);

	map.addRead(4, "eos", 56, 44, 15);
	map.addRead(4, "eos", 56, 44, 142);
	map.addRead(4, "eos", 56, 44, 155);

	// Add unique uid/gid
	map.addRead(98, "mgm", 222, 2424, 5000);

	// Add Write
	map.addWrite(1, "eos", 40, 24, 564);
	map.addWrite(1, "eos", 40, 24, 24250);

	map.addWrite(1, "eos", 42, 24, 125);
	map.addWrite(1, "eos", 42, 24, 2496);
	map.addWrite(1, "eos", 42, 24, 2424);
	map.addWrite(1, "eos", 42, 24, 348);

	map.addWrite(4, "eos", 56, 44, 1425);
	map.addWrite(4, "eos", 56, 44, 14242);
	map.addWrite(4, "eos", 56, 44, 1555);

	map.addWrite(4, "eos", 56, 44, 1425);
	map.addWrite(4, "eos", 56, 44, 14242);
	map.addWrite(4, "eos", 56, 44, 1555);

	map.addWrite(777, "eos", 999, 999, 0);

	// Add unique app
	map.addWrite(1, "xrootd", 123, 123, 542);
	map.addWrite(1, "xrootd", 123, 123, 123);
	map.addWrite(1, "xrootd", 123, 123, 42);

	map.addWrite(1, "xrootd", 42424, 1253, 53);
	map.addWrite(1, "xrootd", 53425, 12243, 24);
	map.addWrite(1, "xrootd", 53244, 12423, 532);

	{
		auto appEmpty= map.getSummary("nullapp");
		auto uidEmpty = map.getSummary(io::TYPE::UID, 123456789);
		auto gidEmpty = map.getSummary(io::TYPE::GID, 123456789);

		if (appEmpty.has_value() || uidEmpty.has_value() || gidEmpty.has_value())
			return -1;
	}
	{
		auto summary = map.getSummary("xrootd");
		// std::cout << summary << std::endl;
		if (summary.has_value() && summary->readBandwidth.has_value() && summary->writeBandwidth.has_value()){
			if ((summary->rSize != 0 || summary->wSize != 6)
				|| static_cast<int>(summary->writeBandwidth->first) != 219
				|| static_cast<int>(summary->writeBandwidth->second) != 226)
				return -1;
		} else
			return -1;
	}
	{
		auto summary = map.getSummary("eos");
		// std::cout << summary << std::endl;
		if (summary.has_value()){
			if ((!summary->readBandwidth.has_value() || !summary->writeBandwidth.has_value())
			|| (static_cast<int>(summary->readBandwidth->first) != 2186 || static_cast<int>(summary->readBandwidth->second) != 6209))
				return -1;
			if (summary->rSize != 14 || summary->wSize != 13)
				return -1;
		} else
			return -1;
	}
	{
		auto summary = map.getSummary(io::TYPE::GID, 999);
		// std::cout << summary << std::endl;
		if (summary.has_value()){
			if (summary->rSize != 0 || summary->wSize != 1)
				return -1;
		} else
			return -1;
	}
	{
		auto summary = map.getSummary(io::TYPE::UID, 222);
		// std::cout << summary << std::endl;
		if (summary.has_value()){
			if (summary->rSize != 1 || summary->wSize != 0 || summary->readBandwidth->first != 5000)
				return -1;
		} else
			return -1;
	}
	return 0;
}

int testIoMapCopy(){
	IoMap map;

	// Calculate raw data
	std::pair<double, double> p1;
	std::pair<double, double> p2;
	std::pair<double, double> p3;

	pairTmp(&p1, &p2, &p3);
	double avrg = 0;
	double deviation = 0;

	// weighted average
	avrg += p1.first * 3;
	avrg += p2.first * 3;
	avrg += p3.first * 3;
	avrg /= 9;

	// weighted standard deviation
	deviation += 3 * (std::pow(p1.second, 2) + std::pow(p1.first - avrg, 2));
	deviation += 3 * (std::pow(p2.second, 2) + std::pow(p2.first - avrg, 2));
	deviation += 3 * (std::pow(p3.second, 2) + std::pow(p3.first - avrg, 2));
	deviation = std::sqrt(deviation / 9);

	// Test IoMap function
	map.addWrite(1, "cernbox", 2, 1, 50);
	map.addWrite(1, "cernbox", 2, 1, 50);
	map.addWrite(1, "cernbox", 2, 1, 26);

	map.addWrite(1, "cernbox", 42, 42, 64);
	map.addWrite(1, "cernbox", 42, 42, 97);
	map.addWrite(1, "cernbox", 42, 42, 34);

	map.addWrite(1, "cernbox", 78, 5, 97);
	map.addWrite(1, "cernbox", 78, 5, 27);
	map.addWrite(1, "cernbox", 78, 5, 44);

	// if no data or difference between raw data and function return -1
	{
		std::optional<std::pair<double, double> > it = map.getBandwidth("cernbox", IoStat::Marks::WRITE);
		if (it.has_value()){
			if (it->first != avrg || it->second != deviation)
				return -1;
		}
		else
			return -1;
	}
	{
		IoMap copyMap(map);

		std::optional<std::pair<double, double> > it = copyMap.getBandwidth("cernbox", IoStat::Marks::WRITE);
		if (it.has_value()){
			if (it->first != avrg || it->second != deviation)
				return -1;
		}
	}
	{
		IoMap copyMap = map;

		std::optional<std::pair<double, double> > it = copyMap.getBandwidth("cernbox", IoStat::Marks::WRITE);
		if (it.has_value()){
			if (it->first != avrg || it->second != deviation)
				return -1;
		}
	}
	return 0;
}
