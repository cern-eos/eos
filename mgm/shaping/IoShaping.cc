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
#include "mgm/FsView.hh"

EOSMGMNAMESPACE_BEGIN

IoShaping::IoShaping(size_t time) : _rReceiving(false),  _rPublishing(false), _rShaping(false),_receivingTime(time){
	eos_static_info("%s", "msg=\"create IoShaping object\"");
}

IoShaping::IoShaping(const IoShaping &other) : _rReceiving(other._rReceiving.load()),
	_rPublishing(other._rPublishing.load()), _rShaping(other._rShaping.load()),
	_receivingTime(other._receivingTime.load()){
		eos_static_info("%s", "msg=\"create IoShaping object by copy constructor\"");
}

IoShaping::~IoShaping(){
	eos_static_info("%s", "msg=\"delete IoShaping object\"");
}

IoShaping& IoShaping::operator=(const IoShaping &other){
	if (this != &other)
		std::scoped_lock lock(_mSyncThread, other._mSyncThread);

	return *this;
}

void IoShaping::receive(ThreadAssistant &assistant) noexcept{
	ThreadAssistant::setSelfThreadName("IoShapingReceiver");
	eos_static_info("%s", "msg=\"starting IoShaping receiving thread\"");

	// wait init ?
	while (!assistant.terminationRequested()){
		assistant.wait_for(std::chrono::seconds(_receivingTime.load()));
		std::lock_guard<std::mutex> lock(_mSyncThread);
		eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
		std::string msg;
		XrdOucString body(msg.c_str());
		std::cerr << "HERE" << std::endl;
		for(auto it = FsView::gFsView.mNodeView.cbegin(); it != FsView::gFsView.mNodeView.cend(); it++)
			std::cerr << it->first << std::endl;
		eos_static_info("%s", "msg=\"IoShaping receiver thread get data\"");
	}

  eos_static_info("%s", "msg=\"stopping IoShaping receiver thread\"");
}

void IoShaping::publish(ThreadAssistant &assistant) noexcept{
	ThreadAssistant::setSelfThreadName("IoShapingPublishing");
	eos_static_info("%s", "msg=\"starting IoShaping publishing thread\"");
	(void)assistant;

}

void IoShaping::shaping(ThreadAssistant &assistant) noexcept{
	ThreadAssistant::setSelfThreadName("IoShaping");
	eos_static_info("%s", "msg=\"starting IoShaping thread\"");
	(void)assistant;
}

bool IoShaping::startReceiving(){
	std::lock_guard<std::mutex> lock(_mSyncThread);

	if (!_rReceiving.load()){
		_rReceiving.store(true);
		_mReceivingThread.reset(&IoShaping::receive, this);
		return true;
	}

	return false;
}

bool IoShaping::stopReceiving(){

	if (_rReceiving.load()){
		std::lock_guard<std::mutex> lock(_mSyncThread);
		_rReceiving.store(false);
		return true;
	}
	return false;
}

bool IoShaping::startPublishing(){
	std::lock_guard<std::mutex> lock(_mSyncThread);

	if (!_rPublishing.load()){
		_rPublishing.store(true);
		_mPublishingThread.reset(&IoShaping::publish, this);
		return true;
	}

	return false;
}

bool IoShaping::stopPublishing(){

	if (_rPublishing.load()){
		std::lock_guard<std::mutex> lock(_mSyncThread);
		_rPublishing.store(false);
		return true;
	}
	return false;
}

bool IoShaping::startShaping(){
	std::lock_guard<std::mutex> lock(_mSyncThread);

	if (!_rShaping.load()){
		_rShaping.store(true);
		_mShapingThread.reset(&IoShaping::shaping, this);
		return true;
	}

	return false;
}

bool IoShaping::stopShaping(){

	if (_rShaping.load()){
		std::lock_guard<std::mutex> lock(_mSyncThread);
		_rShaping.store(false);
		return true;
	}
	return false;
}

void IoShaping::setReceivingTime(size_t time){_receivingTime.store(time);}

EOSMGMNAMESPACE_END
