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
#include "ioMonitor/include/IoMonitor.hh"
#include "ioMonitor/include/IoStat.hh"
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

IoShaping::~IoShaping(){}

IoShaping& IoShaping::operator=(const IoShaping &other){
	if (this != &other)
		std::scoped_lock lock(_mSyncThread, other._mSyncThread);

	return *this;
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
		std::string msg;
		XrdOucString body(msg.c_str());
		for(auto it = FsView::gFsView.mNodeView.cbegin(); it != FsView::gFsView.mNodeView.cend(); it++)
		{
			if (it->second->GetStatus() == "online"){
				std::string node(it->second->GetMember("cfg.stat.hostport"));
				std::string protoMap(it->second->GetMember("cfg.stat.iomap"));
				if (!protoMap.empty())
					_shapings[node] = protoMap;
			}
		}
	}

  eos_static_info("%s", "msg=\"stopping IoShaping receiver thread\"");
}

void IoShaping::publishing(ThreadAssistant &assistant) noexcept{
	ThreadAssistant::setSelfThreadName("IoShapingPublishing");
	eos_static_info("%s", "msg=\"starting IoShaping publishing thread\"");

	assistant.wait_for(std::chrono::seconds(_receivingTime.load()));
	while (!assistant.terminationRequested()){
		if (!_mPublishing.load())
			break ;
		assistant.wait_for(std::chrono::seconds(_receivingTime.load()));
		std::lock_guard<std::mutex> lock(_mSyncThread);
		eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
		for(auto it = FsView::gFsView.mNodeView.cbegin(); it != FsView::gFsView.mNodeView.cend(); it++){
			if (it->second->GetStatus() == "online"){
				/// calcule stat;
				it->second->SetConfigMember("trafic", "0");
				try {
					auto node(_shapings.at(it->second->GetMember("cfg.stat.hostport")));
					if (!node.empty() && node != "0"){
						IoBuffer::summarys trafic;
						google::protobuf::util::JsonParseOptions options;
						auto abslStatus = google::protobuf::util::JsonStringToMessage(node, &trafic, options);
						if (!abslStatus.ok()){
							eos_static_err("%s", "msg=\"Publishing thread, failed to convert node into summarys object\"");
							continue;
						}
						it->second->SetConfigMember("trafic", std::to_string(trafic.aggregated_size()));
					}
				} catch (std::exception e){
					eos_static_err("%s", e.what());
				}
			}
		}
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
		std::string msg;
		XrdOucString body(msg.c_str());
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
		std::lock_guard<std::mutex> lock(_mSyncThread);
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
		std::lock_guard<std::mutex> lock(_mSyncThread);
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
		std::lock_guard<std::mutex> lock(_mSyncThread);
		_mShaping.store(false);
		return true;
	}
	return false;
}

void IoShaping::setReceivingTime(size_t time){_receivingTime.store(time);}

EOSMGMNAMESPACE_END
