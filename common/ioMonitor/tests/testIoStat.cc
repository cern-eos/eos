//  File: testIoStat.cc
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

#include "tester.hh"

void fillIoStat(IoStat &io, int nbr = 10, int range = 100000, double seconds = 0.1){
	if (io::IoStatDebug)
		IoStat::printInfo(std::cout, "fill IoStat");
	for (int i = 0; i < nbr; i++){
		io.add(rand() % range, IoStat::Marks::WRITE);
		io.add(rand() % range * 0.5, IoStat::Marks::READ);
		if (seconds > 0)
			usleep(seconds);
	}
	if (io::IoStatDebug)
		IoStat::printInfo(std::cout, "fill end");
}

int getBandWidth(IoStat &io, IoStat::Marks enumMark, size_t seconds = 10){
	if (io::IoStatDebug){
		std::cout << std::endl;
		IoStat::printInfo(std::cout, "Get bandwidth from the last " + std::to_string(seconds) + "s");
	}

	if (enumMark != IoStat::Marks::READ && enumMark != IoStat::Marks::WRITE)
		return -1;
	
	size_t size = 0;
	std::pair<double, double> bandWidth = io.bandWidth(enumMark, &size, seconds);

	if (io::IoStatDebug){
		std::cout << std::fixed;
		if (enumMark == IoStat::Marks::READ)
			std::cout << "\t[Read:" << size << "/" << io.getSize(enumMark) << "]: average: "
				<< bandWidth.first << " | standard deviation: " << bandWidth.second << std::endl;
		else
			std::cout << "\t[Write:" << size << "/" << io.getSize(enumMark) << "]: average: "
				<< bandWidth.first << " | standard deviation: " << bandWidth.second << std::endl;
		std::cout << std::endl;
	}
	return 0;
}

int cleanMarks(IoStat &io, IoStat::Marks enumMark, int seconds){
	if (io::IoStatDebug)
		IoStat::printInfo(std::cout, "Clean everything after " + std::to_string(seconds) + "s");
	int code = io.cleanOldsMarks(enumMark, seconds);
	if (io::IoStatDebug)
		std::cout << std::endl;
	return code;
}

int testIoStatFillData(){
	IoStat io(4, "mgm", 2, 2);

	fillIoStat(io, 1000000, 100, 0);
	if (getBandWidth(io, IoStat::Marks::READ, 1) < 0
		|| getBandWidth(io, IoStat::Marks::WRITE, 1) < 0)
			return -1;

	if (io::IoStatDebug)
		IoStat::printInfo(std::cout, " [ Error tests ]");
	getBandWidth(io, IoStat::Marks::WRITE, 100);
	size_t size = io.cleanOldsMarks(IoStat::Marks::WRITE, 0);
	if (io::IoStatDebug)
		IoStat::printInfo(std::cout, "Erased " + std::to_string(size) + " element");
	io.add(0, IoStat::Marks::WRITE);
	io.add(0, IoStat::Marks::WRITE);
	io.add(0, IoStat::Marks::WRITE);
	getBandWidth(io, IoStat::Marks::WRITE, 1000);
	getBandWidth(io, IoStat::Marks::WRITE, 0);
	getBandWidth(io, IoStat::Marks::READ, 10);
	io.cleanOldsMarks(IoStat::Marks::READ, 1000);
	getBandWidth(io, IoStat::Marks::READ, 10);
	getBandWidth(io, IoStat::Marks::READ, 0);


	size = io.cleanOldsMarks(IoStat::Marks::WRITE, 0);
	size = io.cleanOldsMarks(IoStat::Marks::READ, 0);
	fillIoStat(io, 1000000, 100, 0);
	for (size_t i = 0; i < 10; i++){
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		getBandWidth(io, IoStat::Marks::WRITE, 1);
		getBandWidth(io, IoStat::Marks::READ, 1);
	}
	return 0;
}

int testIoStatCleaning(){
	IoStat io(4, "qukdb", 2, 2);

	fillIoStat(io, 1000000, 100, 0);
	if (cleanMarks(io, IoStat::Marks::READ, 1) < 0)
		return -1;
	return 0;
}

int testIoStatExactValue(){
	IoStat io1(1, "cernbox", 2, 1);
	IoStat io2(1, "cernbox", 2, 1);
	IoStat io3(1, "cernbox", 2, 1);

	 io1.add(50, IoStat::Marks::READ);
	 io1.add(50, IoStat::Marks::READ);
	 io1.add(26, IoStat::Marks::READ);

	 io2.add(64, IoStat::Marks::READ);
	 io2.add(97, IoStat::Marks::READ);
	 io2.add(34, IoStat::Marks::READ);

	 io3.add(97, IoStat::Marks::READ);
	 io3.add(27, IoStat::Marks::READ);
	 io3.add(44, IoStat::Marks::READ);

	std::pair<double, double> p1 = io1.bandWidth(IoStat::Marks::READ);
	std::pair<double, double> p2 = io2.bandWidth(IoStat::Marks::READ);
	std::pair<double, double> p3 = io3.bandWidth(IoStat::Marks::READ);

	if (p1.first != 42 || p2.first != 65 || p3.first != 56)
		return -1;

	double deviation = 0;
	deviation += std::pow(std::abs(50 - p1.first), 2);
	deviation += std::pow(std::abs(50 - p1.first), 2);
	deviation += std::pow(std::abs(26 - p1.first), 2);
	deviation = std::sqrt(deviation / 3);
	if (deviation != p1.second)
		return -1;
	deviation = 0;
	deviation += std::pow(std::abs(64 - p2.first), 2);
	deviation += std::pow(std::abs(97 - p2.first), 2);
	deviation += std::pow(std::abs(34 - p2.first), 2);
	deviation = std::sqrt(deviation / 3);
	if (deviation != p2.second)
		return -1;
	deviation = 0;
	deviation += std::pow(std::abs(97 - p3.first), 2);
	deviation += std::pow(std::abs(27 - p3.first), 2);
	deviation += std::pow(std::abs(44 - p3.first), 2);
	deviation = std::sqrt(deviation / 3);
	if (deviation != p3.second)
		return -1;
	return 0;
}

int testIoStatCopy(){
	IoStat origin(1, "cernbox", 12, 13);
	IoStat tmp(10, "tmpname", 42, 24);

	origin.add(10, IoStat::Marks::READ);
	origin.add(100, IoStat::Marks::READ);
	origin.add(100, IoStat::Marks::WRITE);
	origin.add(100, IoStat::Marks::WRITE);
	origin.add(100, IoStat::Marks::WRITE);
	tmp = origin;
	if (tmp.getApp() != "cernbox"
		|| tmp.getUid() != 12 || tmp.getGid() != 13
		|| tmp.getSize(IoStat::Marks::READ) != 2
		|| tmp.getSize(IoStat::Marks::WRITE) != 3)
		return -1;
	return 0;
}

int testIoStatIOPS(){
	IoStat io(1, "eos", 1, 1);
	double rAvrg = 0;
	double wAvrg = 0;

	srand((unsigned int)time(NULL));
	for (size_t i = 0; i < 10; i++){
		double read = std::abs(rand()) % 100;
		double write = std::abs(rand()) % 100;
		rAvrg += read;
		wAvrg += write;

		for (size_t j = 0; j < read; j++)
			io.add((std::abs(rand()) % 10000), IoStat::Marks::READ);
		for (size_t j = 0; j < write; j++)
			io.add((std::abs(rand()) % 10000), IoStat::Marks::WRITE);
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
	rAvrg /= 10;
	wAvrg /= 10;

	if (rAvrg != io.getIOPS(IoStat::Marks::READ)
		|| wAvrg != io.getIOPS(IoStat::Marks::WRITE))
		return -1;
	return 0;
}
