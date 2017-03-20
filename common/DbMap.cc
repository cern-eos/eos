// ----------------------------------------------------------------------
// File: DbMap.cc
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

#include "common/DbMap.hh"
#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN
/*-------------------- EXPLICIT INSTANTIATIONS -------------------------------*/
template class DbMapT<LvDbDbMapInterface, LvDbDbLogInterface>;
template class DbLogT<LvDbDbMapInterface, LvDbDbLogInterface>;
/*----------------------------------------------------------------------------*/

/*-------------- IMPLEMENTATIONS OF STATIC MEMBER VARIABLES ------------------*/
template<class A, class B> set<string> DbMapT<A, B>::gNames;
template<class A, class B> eos::common::RWMutex DbMapT<A, B>::gNamesMutex;
template<class A, class B> eos::common::RWMutex DbMapT<A, B>::gTimeMutex;
template<class A, class B> bool DbMapT<A, B>::gInitialized = false;
template<class A, class B> size_t DbMapT<A, B>::pDbIterationChunkSize = 10000;

/*----------------------------------------------------------------------------*/
typedef DbMapT<LvDbDbMapInterface, LvDbDbLogInterface> DbMapLeveldb;
typedef DbLogT<LvDbDbLogInterface, LvDbDbLogInterface> DbLogLeveldb;

EOSCOMMONNAMESPACE_END

