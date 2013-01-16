// ----------------------------------------------------------------------
// File: RWMutex.cc
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/RWMutex.hh"

#ifdef EOS_INSTRUMENTED_RWMUTEX
EOSCOMMONNAMESPACE_BEGIN
size_t RWMutex::cumulatedwaitread_static=0;
size_t RWMutex::cumulatedwaitwrite_static=0;
size_t RWMutex::maxwaitread_static=0;
size_t RWMutex::maxwaitwrite_static=0;
size_t RWMutex::minwaitread_static=1e12;
size_t RWMutex::minwaitwrite_static=1e12;
size_t RWMutex::readLockCounterSample_static=0;
size_t RWMutex::writeLockCounterSample_static=0;
size_t RWMutex::timingCompensation=0;
size_t RWMutex::timingLatency=0;
size_t RWMutex::orderCheckingLatency=0;
size_t RWMutex::lockUnlockDuration=0;
int RWMutex::samplingModulo_static=(int) (0.01*RAND_MAX);
bool RWMutex::staticInitialized=false;
bool RWMutex::enabletimingglobal=false;
bool RWMutex::enableordercheckglobal=false;
RWMutex::rules_t RWMutex::rules_static;
std::map<unsigned char,std::string> RWMutex::ruleIndex2Name_static;
std::map<std::string,unsigned char> RWMutex::ruleName2Index_static;
__thread bool *RWMutex::orderCheckReset_staticthread=NULL;
__thread unsigned long RWMutex::ordermask_staticthread[EOS_RWMUTEX_ORDER_NRULES];
std::map<pthread_t,bool> RWMutex::threadOrderCheckResetFlags_static;
pthread_rwlock_t RWMutex::orderChkMgmLock;
EOSCOMMONNAMESPACE_END
#endif
