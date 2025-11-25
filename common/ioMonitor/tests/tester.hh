#include <algorithm>
#include <gtest/gtest.h>
#include <iostream>
#include "../include/IoAggregateMap.hh"
#include <google/protobuf/util/json_util.h>

#define TIME_TO_FILL 5

int testIoStatFillData();
int testIoStatCleaning();
int testIoStatExactValue();
int testIoStatIOPS();
int testIoStatCopy();

int testInteractiveIoMap();
int testIoMapBigVolume();
int testIoMapSpecificCase();
int testIoMapExactValue();
int testIoMapData();
int testIoMapSummary();
int testIoMapIds();
int testIoMapCopy();


int testIoAggregateMap();
int testIoAggregateMapInteract();
int testIoAggregateMapWindow();
int testIoAggregateMapCopy();
int testIoAggregateMapDelete();

int testIoAggregateCopy();

int testIoBuffer();

template<typename T>
void fillData(T &map){
	for (size_t i = 0; i < 10; i++){
		int uid = std::abs(rand() % 100);
		int gid = std::abs(rand() % 100);
		for (size_t it = 0, max = std::abs(rand())% 100; it < max;it++){
			size_t bytes = std::abs(rand() % 100000);
			map.addRead(i, "mgm", uid, gid, bytes);
			map.addWrite(i, "mgm", uid, gid, bytes * 3);
		}
	}
	for (size_t i = 10; i < 20; i++){
		int uid = std::abs(rand() % 100);
		int gid = std::abs(rand() % 100);
		for (size_t it = 0, max = std::abs(rand()) % 100; it < max;it++){
			size_t bytes = std::abs(rand() % 100000);
			map.addRead(i, "fdf", uid, gid, bytes);
			map.addWrite(i, "fdf", uid, gid, bytes * 4);
		}
	}
	for (size_t i = 20; i < 30; i++){
		int uid = std::abs(rand() % 100);
		int gid = std::abs(rand() % 100);
		for (size_t it = 0, max = std::abs(rand()) % 100; it < max;it++){
			size_t bytes = std::abs(rand() % 100000);
			map.addRead(i, "miniRT", uid, gid, bytes);
			map.addWrite(i, "miniRT", uid, gid, bytes * 9);
		}
	}
}

template<typename T>
void fillThread(T &map, std::mutex &mutex,
			   size_t nbrOfLoop,
			   size_t fileId,
			   std::string appName,
			   size_t maxInteraction,
			   size_t maxByte,
			   size_t uid,
			   size_t gid,
			   bool rw){
	for (size_t i = 0; i < nbrOfLoop; i++){
		std::this_thread::sleep_for(std::chrono::seconds(1));
		std::lock_guard<std::mutex> lock(mutex);
		for (size_t j = (std::abs(rand()) % maxInteraction); j < maxInteraction; j++){
			if (!rw)
				map.addRead(fileId, appName, uid, gid, std::abs(rand()) % maxByte);
			else
				map.addWrite(fileId, appName, uid, gid, std::abs(rand()) % maxByte);
		}
	}
}

template<typename T>
int fillDataInteract(T &map, std::mutex &mutex){
	std::string input;
	std::string appName;
	size_t fileId = 0;
	size_t uid = 0;
	size_t gid = 0;
	size_t nbrOfLoop = 0;
	size_t maxInteraction = 0;
	size_t maxByte = 0;
	bool rw = false;
	bool bg = false;
	std::thread bgThread;

	std::cout << "Write ran for random data" << std::endl;
	while (true){
		try {
			std::cout << "fileId: ";
			std::getline(std::cin, input);
			if (input == "ran")
				fileId = std::abs(rand()) % 100;
			else
				fileId = std::stoi(input);
			std::cout << "\033[F\033[K";
			std::cout << "appName: ";
			std::getline(std::cin, appName);
			std::cout << "\033[F\033[K";
			std::cout << "uid: ";
			std::getline(std::cin, input);
			if (input == "ran")
				uid = std::abs(rand()) % 100;
			else
				uid = std::stoi(input);
			std::cout << "\033[F\033[K";
			std::cout << "gid: ";
			std::getline(std::cin, input);
			if (input == "ran")
				gid = std::abs(rand()) % 100;
			else
				gid = std::stoi(input);
			std::cout << "\033[F\033[K";
			std::cout << "number of loop: ";
			std::getline(std::cin, input);
			if (input == "ran")
				nbrOfLoop = std::abs(rand()) % 10;
			else
				nbrOfLoop = std::stoi(input);
			std::cout << "\033[F\033[K";
			std::cout << "max iteration/loop: ";
			std::getline(std::cin, input);
			if (input == "ran")
				maxInteraction = std::abs(rand()) % 200;
			else
				maxInteraction = std::stoi(input);
			std::cout << "\033[F\033[K";
			std::cout << "max Bytes: ";
			std::getline(std::cin, input);
			if (input == "ran")
				maxByte = std::abs(rand()) % 10000;
			else
				maxByte = std::stoi(input);
			std::cout << "\033[F\033[K";
			std::cout << "Read or Write[r/w]: ";


			while (std::getline(std::cin, input)){
				if (input == "r" || input == "w"){
					if (input == "r")
						rw = false;
					else if (input == "w")
						rw = true;
					break;
				}
				std::cout << "\033[F\033[K";
				std::cout << "Read or Write[r/w]: ";
			}
			std::cout << "\033[F\033[K";
			std::cout << "Run in background[y/n]: ";
			while (std::getline(std::cin, input)){
				if (input == "y" || input == "n"){
					if (input == "y")
						bg = true;
					break;
				}
				std::cout << "\033[F\033[K";
				std::cout << "Run in background[y/n]: ";
			}
			if (!bg){
				std::lock_guard<std::mutex> lock(mutex);
				for (size_t i = 0; i < nbrOfLoop; i++){
					for (size_t j = (std::abs(rand()) % maxInteraction); j < maxInteraction; j++){
						if (!rw)
							map.addRead(fileId, appName, uid, gid, std::abs(rand()) % maxByte);
						else
							map.addWrite(fileId, appName, uid, gid, std::abs(rand()) % maxByte);
					}
					std::cout << "\033[F\033[K";
					std::cout << "fill the map[" << (i + 1) << "/" << nbrOfLoop << "]" << std::endl;
					std::this_thread::sleep_for(std::chrono::seconds(1));
				}
			}else{
				bgThread = std::thread([&, nbrOfLoop, fileId, appName, maxInteraction, maxByte, uid, gid, rw]() {
					fillThread(map,
						   mutex,
						   nbrOfLoop,
						   fileId,
						   appName,
						   maxInteraction,
						   maxByte,
						   uid,
						   gid,
						   rw);
				});
				bgThread.detach();
			}
			break ;
		} catch(std::exception &e){
			std::cout << "\033[F\033[K";
			std::cerr << "Monitor: Error: " << e.what() << ": Bad input" << std::endl;
			std::cout << "Exit[y/n]: ";
			while (std::getline(std::cin, input)){
				if (input == "y" || input == "n"){
					if (input == "y")
						return -1;
					break;
				}
				std::cout << "\033[F\033[K";
				std::cout << "Exit[y/n]: ";
			}
		}
	}

	return 0;
}

std::ostream& operator<<(std::ostream &os, const std::pair<double, double> &other);
std::ostream& operator<<(std::ostream &os, const std::optional<IoStatSummary> &opt);
