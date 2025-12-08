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
#include "ioMonitor/include/IoAggregateMap.hh"
#include "mgm/FsView.hh"

EOSMGMNAMESPACE_BEGIN

IoShaping::IoShaping(size_t time) : _mReceiving(false),  _mPublishing(false),
  _mShaping(false),_receivingTime(time){
	_scaler.add_windows(10);
	_scaler.add_windows(60);
	_scaler.add_windows(300);
}

IoShaping::IoShaping(const IoShaping &other){
	if (this != &other){
		std::scoped_lock lock(other._mSyncThread, _mSyncThread);
		_mReceiving.store(other._mReceiving.load());
		_mPublishing.store(other._mPublishing.load());
		_mShaping.store(other._mShaping.load());
		_receivingTime.store(other._receivingTime.load());

		_shapings = other._shapings;
		_scaler = other._scaler;
		_limiter = other._limiter;
		if (_mPublishing.load())
			startPublishing();
		if (_mReceiving.load())
			startReceiving();
		if (_mShaping.load())
			startShaping();
	}
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
	if (this != &other){
		std::scoped_lock lock(other._mSyncThread, _mSyncThread);
		_mReceiving.store(other._mReceiving.load());
		_mPublishing.store(other._mPublishing.load());
		_mShaping.store(other._mShaping.load());
		_receivingTime.store(other._receivingTime.load());

		_shapings = other._shapings;
		_scaler = other._scaler;
		_limiter = other._limiter;
		if (_mPublishing.load())
			startPublishing();
		if (_mReceiving.load())
			startReceiving();
		if (_mShaping.load())
			startShaping();
	}

  return *this;
}

IoBuffer::summarys IoShaping::aggregateSummarys(std::vector<IoBuffer::summarys> &received){
	IoBuffer::summarys	final;
	std::map<uint64_t, std::map<std::string, std::vector<IoStatSummary> > > apps;
	std::map<uint64_t, std::map<gid_t, std::vector<IoStatSummary> > > uids;
	std::map<uint64_t, std::map<uid_t, std::vector<IoStatSummary> > > gids;

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
		std::string publish;
		google::protobuf::util::JsonPrintOptions options;

		auto abslStatus = google::protobuf::util::MessageToJsonString(_scaler, &publish, options);
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

bool IoShaping::calculeScalerNodes(){
	eos_static_info("msg=\"Calculate the scaler begin\"");

	if (_shapings.aggregated_size() <= 0)
		return false;


	size_t winTime = !_shapings.aggregated().empty() ?
		std::min_element(_shapings.aggregated().begin(), _shapings.aggregated().end(),
				   [](auto &a, auto &b){return a.first < b.first;})->first : 0;
	if (winTime == 0)
		return false;
	auto &it = _shapings.aggregated().at(winTime);
	for (auto apps : it.apps()){
		if ((_limiter.rApps.find(apps.first) != _limiter.rApps.end())
			&& apps.second.ravrg()
			&& _limiter.rApps[apps.first].second / (apps.second.ravrg() * apps.second.riops()) < 1
			&& _limiter.rApps[apps.first].first)
				(*_scaler.mutable_apps()->mutable_read())[apps.first]
				= static_cast<float>(_limiter.rApps[apps.first].second) / (apps.second.ravrg() * apps.second.riops());
		else
			(*_scaler.mutable_apps()->mutable_read())[apps.first] = 1.0;
		// eos_static_info("msg=\"app: %s, limit: %d, running: %d, division: %f, value: %f\"",
		// 		apps.first.c_str(), _limiter.rApps[apps.first].second, _limiter.rApps[apps.first].first, _limiter.rApps[apps.first].second / (apps.second.ravrg() * apps.second.riops()),
		// 	   apps.second.ravrg() * apps.second.riops());

		if ((_limiter.wApps.find(apps.first) != _limiter.wApps.end())
			&& apps.second.wavrg()
			&& _limiter.wApps[apps.first].second / apps.second.wavrg() < 1
			&& _limiter.wApps[apps.first].first)
				(*_scaler.mutable_apps()->mutable_write())[apps.first] =
					static_cast<float>(_limiter.wApps[apps.first].second) / apps.second.wavrg();
		else
			_scaler.mutable_apps()->mutable_write()->insert({apps.first, 1.0});
	}
	for (auto uids : it.uids()){
		if ((_limiter.rUids.find(uids.first) != _limiter.rUids.end())
			&& (uids.second.ravrg()
			&& _limiter.rUids[uids.first].second / uids.second.ravrg() < 1
			&& _limiter.rUids[uids.first].first))
				(*_scaler.mutable_uids()->mutable_read())[uids.first] =
					static_cast<float>(_limiter.rUids[uids.first].second) / uids.second.ravrg();
		else
			_scaler.mutable_uids()->mutable_read()->insert({uids.first, 1.0});

		if ((_limiter.wUids.find(uids.first) != _limiter.wUids.end())
			&& uids.second.wavrg()
			&& _limiter.wUids[uids.first].second / uids.second.wavrg() < 1
			&& _limiter.wUids[uids.first].first)
				(*_scaler.mutable_uids()->mutable_write())[uids.first] =
					static_cast<float>(_limiter.wUids[uids.first].second) / uids.second.wavrg();
		else
			_scaler.mutable_uids()->mutable_write()->insert({uids.first, 1.0});
	}
	for (auto gids : it.gids()){
		if ((_limiter.rGids.find(gids.first) != _limiter.rGids.end())
			&& gids.second.ravrg()
			&& _limiter.rGids[gids.first].second / gids.second.ravrg() < 1
			&& _limiter.rGids[gids.first].first)
				(*_scaler.mutable_gids()->mutable_read())[gids.first] =
					static_cast<float>(_limiter.rGids[gids.first].second) / gids.second.ravrg();
		else
			_scaler.mutable_gids()->mutable_read()->insert({gids.first, 1.0});

		if ((_limiter.wGids.find(gids.first) != _limiter.wGids.end())
			&& gids.second.wavrg()
			&& _limiter.wGids[gids.first].second / gids.second.wavrg() < 1
			&& _limiter.wGids[gids.first].first)
				(*_scaler.mutable_gids()->mutable_write())[gids.first] =
					static_cast<float>(_limiter.wGids[gids.first].second) / gids.second.wavrg();
		else
			_scaler.mutable_gids()->mutable_write()->insert({gids.first, 1.0});
	}

	return true;
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

		if (!calculeScalerNodes()){
			eos_static_err("msg=\"Calcule scaler failed\"");
			continue;
		}
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

Limiter IoShaping::getLimiter() const{
    std::lock_guard<std::mutex> lock(_mSyncThread);
	return _limiter;
}

EOSMGMNAMESPACE_END
