//  File: testIoBuffer.cc
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


int testIoBuffer(){
	IoAggregateMap map;
	IoBuffer::data proto;
	std::vector<IoStatSummary> apps;
	std::vector<IoStatSummary> uids;
	std::vector<IoStatSummary> gids;
	size_t idU = std::abs(rand() % 100);
	size_t idG = std::abs(rand() % 100);
	
	// Add window
	map.addWindow(3600);

	// set Tracks
	map.setTrack(3600, "eos");
	map.setTrack(3600, "mgm");

	map.setTrack(3600, io::TYPE::UID, idU);
	map.setTrack(3600, io::TYPE::GID, idG);

	map.setTrack(3600, io::TYPE::UID, 12);
	map.setTrack(3600, io::TYPE::GID, 12);

	for (size_t win = 0; win < 5; win++){
		std::cout << "Add summarys[" << (win + 1) << "/" << 5 << "]" << std::endl << std::endl;
		for (size_t i = 0; i < 10; i++){
			for (size_t j = (std::abs(rand()) % 100); j < 100; j++){
				map.addRead(1, "eos", idU, idG, std::abs(rand()) % 10000);
				map.addRead(1, "mgm", 12, 13, std::abs(rand()) % 10000);

				map.addRead(1, "eos", idU, idG, std::abs(rand()) % 10000);
				map.addRead(1, "eos", 12, idG, std::abs(rand()) % 10000);
				map.addRead(1, "eawdos", 133, 12, std::abs(rand()) % 10000);
			}
			std::cout << "\033[F\033[K";
			std::cout << "\tfill the map[" << (i + 1) << "/" << 10 << "]" << std::endl;
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
		{
			auto eos(map.getSummary(3600, "eos"));
			if (!eos.has_value()){
				std::cerr << "appName no value" << std::endl;
				return -1;
			}
			apps.push_back(eos.value());
		}
		{
			auto uid(map.getSummary(3600, io::TYPE::UID, idU));
			if (!uid.has_value()){
				std::cerr << "uids no value" << std::endl;
				return -1;
			}
			uids.push_back(uid.value());
		}
		{
			auto gid(map.getSummary(3600, io::TYPE::GID, idG));
			if (!gid.has_value()){
				std::cerr << "gids no value" << std::endl;
				return -1;
			}
			gids.push_back(gid.value());
		}
		std::cout << "\033[F\033[K";
		std::cout << "\033[F\033[K";
	}
	IoBuffer::Summary protoBuf;
	proto.mutable_apps()->emplace("eos", IoAggregate::summaryWeighted(apps, 3600)->Serialize(protoBuf));
	protoBuf.Clear();
	proto.mutable_uids()->emplace(12, IoAggregate::summaryWeighted(uids, 3600)->Serialize(protoBuf));
	protoBuf.Clear();
	proto.mutable_gids()->emplace(11, IoAggregate::summaryWeighted(gids, 3600)->Serialize(protoBuf));
	apps.clear();
	uids.clear();
	gids.clear();
	{
		auto eos(map.getSummary(3600, "mgm"));
		if (!eos.has_value()){
			std::cerr << "appName no value" << std::endl;
			return -1;
		}
		apps.push_back(eos.value());
	}
	{
		auto uid(map.getSummary(3600, io::TYPE::UID, 12));
		if (!uid.has_value()){
			std::cerr << "uids no value" << std::endl;
			return -1;
		}
		uids.push_back(uid.value());
	}
	{
		auto gid(map.getSummary(3600, io::TYPE::GID, 12));
		if (!gid.has_value()){
			std::cerr << "gids no value" << std::endl;
			return -1;
		}
		gids.push_back(gid.value());
	}
	proto.mutable_apps()->emplace("mgm", IoAggregate::summaryWeighted(apps, 3600)->Serialize(protoBuf));
	protoBuf.Clear();
	proto.mutable_uids()->emplace(12, IoAggregate::summaryWeighted(uids, 3600)->Serialize(protoBuf));
	protoBuf.Clear();
	proto.mutable_gids()->emplace(12, IoAggregate::summaryWeighted(gids, 3600)->Serialize(protoBuf));

	std::string output;
	{
		google::protobuf::util::JsonPrintOptions options;
		options.add_whitespace = true;
		options.always_print_primitive_fields = true;
		options.preserve_proto_field_names = true;
		auto it = google::protobuf::util::MessageToJsonString(proto, &output, options);
		if (!it.ok())
			return -1;
		std::cout << "JSON:" << std::endl << output << std::endl;
	}
	{
		IoBuffer::data back;
		google::protobuf::util::JsonParseOptions options;
		auto it = google::protobuf::util::JsonStringToMessage(output, &back, options);
		if (!it.ok())
			return -1;
		// std::cout << "JSON DEBUG:" << std::endl << back.DebugString() << std::endl;
	}

	google::protobuf::ShutdownProtobufLibrary();
	return 0;
}
