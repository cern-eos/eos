//  File: IoShaping.cc
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

#include "IoShaping.hh"
#include "FileSystem.hh"
#include "ioMonitor/include/IoAggregateMap.hh"
#include "mgm/FsView.hh"
#include "qclient/shared/SharedHashSubscription.hh"

EOSMGMNAMESPACE_BEGIN

IoShaping::IoShaping(size_t time) : _mReceiving(false),  _mPublishing(false),
  _mShaping(false),_receivingTime(time){
}

IoShaping::IoShaping(const IoShaping &other) : _mReceiving(other._mReceiving.load()),
  _mPublishing(other._mPublishing.load()), _mShaping(other._mShaping.load()),
  _receivingTime(other._receivingTime.load()){
}

IoShaping::~IoShaping(){
  if (_mShaping.load()){
  	std::lock_guard<std::mutex> lock(_mSyncThread);
  	_mShaping.store(false);
  }
  if (_mPublishing.load()){
    std::lock_guard<std::mutex> lock(_mSyncThread);
	_mPublishing.store(false);
  }
  if (_mReceiving.load()){
	std::lock_guard<std::mutex> lock(_mSyncThread);
	_mReceiving.store(false);
  }
}

IoShaping& IoShaping::operator=(const IoShaping &other){
  if (this != &other)
	std::scoped_lock lock(_mSyncThread, other._mSyncThread);

  return *this;
}

IoBuffer::summarys IoShaping::aggregateSummarys(std::vector<IoBuffer::summarys> &received){
	IoBuffer::summarys	final;
	std::map<uint64_t, std::map<std::string, std::vector<IoStatSummary> > > apps;
	std::map<uint64_t, std::map<gid_t, std::vector<IoStatSummary> > > uids;
	std::map<uint64_t, std::map<uid_t, std::vector<IoStatSummary> > > gids;
	eos_static_info("msg=\"aggregateSummarys begin\"");

	for (auto it = received.begin(); it != received.end(); it++){
		auto aggregate = it->mutable_aggregated();
		for (auto window = aggregate->begin(); window != aggregate->end(); window++){
			auto mutableApps = window->second.mutable_apps();
			for (auto appMap = mutableApps->begin(); appMap != mutableApps->end(); appMap++){
				apps[window->first][appMap->first].push_back(IoStatSummary(appMap->second));
			}

			auto mutableUids = window->second.mutable_uids();
			for (auto uidMap = mutableUids->begin(); uidMap != mutableUids->end(); uidMap++){
				uids[window->first][uidMap->first].push_back(IoStatSummary(uidMap->second));
			}

			auto mutableGids = window->second.mutable_gids();
			for (auto gidMap = mutableGids->begin(); gidMap != mutableGids->end(); gidMap++){
				gids[window->first][gidMap->first].push_back(IoStatSummary(gidMap->second));
			}
		}
	}


	for (auto window : apps){
		IoBuffer::data data;
		if (!final.aggregated().contains(window.first)){

			auto mutableApps = data.mutable_apps();
			for (auto appName : window.second){
				auto sum = IoAggregate::summaryWeighted(appName.second, window.first);
				if (sum.has_value()){
					IoBuffer::Summary buffer;
					sum->Serialize(buffer);
					mutableApps->insert({appName.first, buffer});
				}
			}
			final.mutable_aggregated()->insert({window.first, data});
		}else{
			auto mutableApps = final.mutable_aggregated()->at(window.first).mutable_apps();
			for (auto appName : window.second){
				auto sum = IoAggregate::summaryWeighted(appName.second, window.first);
				if (sum.has_value()){
					IoBuffer::Summary buffer;
					sum->Serialize(buffer);
					mutableApps->insert({appName.first, buffer});
				}
			}
		}
	}
	for (auto window : uids){
		IoBuffer::data data;
		if (!final.aggregated().contains(window.first)){

			auto mutableUids = data.mutable_uids();
			for (auto uidName : window.second){
				auto sum = IoAggregate::summaryWeighted(uidName.second, window.first);
				if (sum.has_value()){
					IoBuffer::Summary buffer;
					sum->Serialize(buffer);
					mutableUids->insert({uidName.first, buffer});
				}
			}
			final.mutable_aggregated()->insert({window.first, data});
		}else{
			auto mutableUids = final.mutable_aggregated()->at(window.first).mutable_uids();
			for (auto uidName : window.second){
				auto sum = IoAggregate::summaryWeighted(uidName.second, window.first);
				if (sum.has_value()){
					IoBuffer::Summary buffer;
					sum->Serialize(buffer);
					mutableUids->insert({uidName.first, buffer});
				}
			}
		}
	}
	for (auto window : gids){
		IoBuffer::data data;
		if (!final.aggregated().contains(window.first)){

			auto mutableGids = data.mutable_gids();
			for (auto gidName : window.second){
				auto sum = IoAggregate::summaryWeighted(gidName.second, window.first);
				if (sum.has_value()){
					IoBuffer::Summary buffer;
					sum->Serialize(buffer);
					mutableGids->insert({gidName.first, buffer});
				}
			}
			final.mutable_aggregated()->insert({window.first, data});
		}else{
			auto mutableGids = final.mutable_aggregated()->at(window.first).mutable_gids();
			for (auto gidName : window.second){
				auto sum = IoAggregate::summaryWeighted(gidName.second, window.first);
				if (sum.has_value()){
					IoBuffer::Summary buffer;
					sum->Serialize(buffer);
					mutableGids->insert({gidName.first, buffer});
				}
			}
		}
	}

	return final;
}

void IoShaping::receive(ThreadAssistant &assistant) noexcept{
  ThreadAssistant::setSelfThreadName("IoShapingReceiver");
  eos_static_info("%s", "msg=\"starting IoShaping receiving thread\"");

  assistant.wait_for(std::chrono::seconds(2));
  while (!assistant.terminationRequested()){
	if (!_mReceiving.load())
	  break ;
	assistant.wait_for(std::chrono::seconds(_receivingTime.load()));

	std::lock_guard<std::mutex> lock(_mSyncThread);
	eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);

	std::vector<IoBuffer::summarys> sums;
	IoBuffer::summarys received;

	for(auto it = FsView::gFsView.mNodeView.cbegin(); it != FsView::gFsView.mNodeView.cend(); it++)
	{
	  if (it->second->GetStatus() == "online"){
		std::string node(it->second->GetMember("cfg.stat.hostport"));
		std::string protoMap(it->second->GetMember("cfg.stat.iomap"));
		if (protoMap != "0"){
		  google::protobuf::util::JsonParseOptions options;
		  auto it = google::protobuf::util::JsonStringToMessage(protoMap, &received, options);
		  if (!it.ok()){
		    eos_static_err("msg=\"Protobuff received error\"");
			continue;
		  }
		  sums.push_back(received);
		  received.Clear();
		}
	  }
    }
	if (!sums.empty()){
		_shapings = aggregateSummarys(sums);
		std::string out;
		google::protobuf::util::JsonPrintOptions options;
		auto it = google::protobuf::util::MessageToJsonString(_shapings, &out, options);
		if (!it.ok())
			eos_static_err("msg=\"ProtoBuff _shaping failed\"");
	} else{
		if (_shapings.aggregated_size() > 0)
			_shapings.Clear();
		eos_static_info("msg=\"No data\"");
	}
  }
  eos_static_info("%s", "msg=\"stopping IoShaping receiver thread\"");
}

bool IoShaping::calculeScalerNodes(Shaping::Scaler &scaler) const{

	if (_shapings.aggregated_size() <= 0)
		return false;

	for (auto it : _shapings.aggregated()){
		scaler.add_windows(it.first);
		for (auto apps : it.second.apps()){
			// float scaler = 0;
		}

		for (auto apps : it.second.uids()){}

		for (auto apps : it.second.gids()){}
	}

	return true;
}

void IoShaping::publishing(ThreadAssistant &assistant){
	ThreadAssistant::setSelfThreadName("IoShapingPublishing");
	eos_static_info("%s", "msg=\"starting IoShaping publishing thread\"");

	assistant.wait_for(std::chrono::seconds(2));
	while (!assistant.terminationRequested()){
		if (!_mPublishing.load())
			break ;
		assistant.wait_for(std::chrono::seconds(_receivingTime.load()));
		std::lock_guard<std::mutex> lock(_mSyncThread);
		eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
		Shaping::Scaler scaler;
		std::string publish;
		google::protobuf::util::JsonPrintOptions options;

		if (_shapings.aggregated().empty()){
			eos_static_info("msg=\"Nothing to scale\"");
			continue;
		}

		if (!calculeScalerNodes(scaler)){
			eos_static_err("msg=\"Calcule scaler failed\"");
			continue;
		}
		_scaler = scaler;
		auto abslStatus = google::protobuf::util::MessageToJsonString(scaler, &publish, options);
		if (!abslStatus.ok()){
			eos_static_err("%s", "msg=\"Failed to convert Shaping::Scaler object to JSON String\"");
			continue;
		}

		for(auto it = FsView::gFsView.mNodeView.cbegin(); it != FsView::gFsView.mNodeView.cend(); it++)
		  if (it->second->GetStatus() == "online")
			it->second->SetConfigMember("stat.scaler.xyz", publish.c_str(), true);
	}

  eos_static_info("%s", "msg=\"stopping IoShaping publishing thread\"");
}

void IoShaping::shaping(ThreadAssistant &assistant) noexcept{
	ThreadAssistant::setSelfThreadName("IoShaping");
	eos_static_info("%s", "msg=\"starting IoShaping shaping thread\"");
	assistant.wait_for(std::chrono::seconds(_receivingTime.load()));
	while (!assistant.terminationRequested()){
		if (!_mPublishing.load())
			break ;
		assistant.wait_for(std::chrono::seconds(_receivingTime.load()));
		std::lock_guard<std::mutex> lock(_mSyncThread);
		eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
	}

  eos_static_info("%s", "msg=\"stopping IoShaping publishing thread\"");
}

bool IoShaping::startReceiving(){
  std::lock_guard<std::mutex> lock(_mSyncThread);

  if (!_mReceiving.load()){
	_mReceiving.store(true);
	_mReceivingThread.reset(&IoShaping::receive, this);
	return true;
  }

  return false;
}

bool IoShaping::stopReceiving(){

  if (_mReceiving.load()){
	_mReceiving.store(false);
	return true;
  }
  return false;
}

bool IoShaping::startPublishing(){
  std::lock_guard<std::mutex> lock(_mSyncThread);

  if (!_mPublishing.load()){
	_mPublishing.store(true);
	_mPublishingThread.reset(&IoShaping::publishing, this);
	return true;
  }
  return false;
}

bool IoShaping::stopPublishing(){

  if (_mPublishing.load()){
	_mPublishing.store(false);
	return true;
  }
  return false;
}

bool IoShaping::startShaping(){
  std::lock_guard<std::mutex> lock(_mSyncThread);

  if (!_mShaping.load()){
	_mShaping.store(true);
	_mShapingThread.reset(&IoShaping::shaping, this);
	return true;
  }
  return false;
}

bool IoShaping::stopShaping(){

  if (_mShaping.load()){
	_mShaping.store(false);
	return true;
  }
  return false;
}

void IoShaping::setReceivingTime(size_t time){_receivingTime.store(time);}

IoBuffer::summarys IoShaping::getShaping() const{
    std::lock_guard<std::mutex> lock(_mSyncThread);
	return _shapings;
}

Shaping::Scaler IoShaping::getScaler() const{
    std::lock_guard<std::mutex> lock(_mSyncThread);
	return _scaler;
}

EOSMGMNAMESPACE_END
