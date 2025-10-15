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
#include <chrono>
#include <mutex>

EOSMGMNAMESPACE_BEGIN

IoShaping::IoShaping(size_t time) : _rReceiving(false),  _rPublishing(false), _rShaping(false),_receivingTime(time){}

IoShaping::IoShaping(const IoShaping &other) : _rReceiving(other._rReceiving.load()),
	_rPublishing(other._rPublishing.load()), _rShaping(other._rShaping.load()),
	_receivingTime(other._receivingTime.load())
{}

IoShaping& IoShaping::operator=(const IoShaping &other){
	if (this != &other){
		std::scoped_lock lock(_mSyncThread, other._mSyncThread);
	}

	return *this;
}

void IoShaping::setReceivingTime(size_t time){_receivingTime.store(time);}

void IoShaping::receive(ThreadAssistant &assistant) noexcept{
	ThreadAssistant::setSelfThreadName("IoShapingReceiver");
	eos_static_info("%s", "msg=\"starting IoShaping receive thread\"");

	if (gOFS == nullptr) {
		return;
	}
	// wait init ?

	while (!assistant.terminationRequested()){
		std::lock_guard<std::mutex> lock(_mSyncThread);
		std::string msg;
		XrdOucString body(msg.c_str());
		/// get all data
		eos_static_info("%s", "msg=\"IoShaping receiver thread get data\"");
		assistant.wait_for(std::chrono::seconds(_receivingTime.load()));
	}

  eos_static_info("%s", "msg=\"stopping IoShaping receiver thread\"");
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

IoShaping::~IoShaping(){}

EOSMGMNAMESPACE_END
