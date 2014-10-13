//------------------------------------------------------------------------------
// @file SchedulingTreeCommon.cc
// @author Geoffray Adde - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#define DEFINE_TREECOMMON_MACRO
#include "mgm/geotree/SchedulingTreeCommon.hh"
#include <iomanip>

using namespace std;

EOSMGMNAMESPACE_BEGIN

// static variables implementation
SchedTreeBase::Settings SchedTreeBase::gSettings =
{	0 , 0};

ostream& SchedTreeBase::TreeNodeInfo::display(ostream &os) const
{
	if(nodeType==intermediate)
	os << "nodetype=intermediate" << " , ";
	else if(nodeType==fs)
	os << "nodetype=fs          " << " , ";
	else
	os << "nodetype=unknown!    " << " , ";
	os << "geotag=" << setw(8) << setfill(' ') << geotag<< " , ";
	os << "fullgeotag=" << setw(8) << setfill(' ') << fullGeotag<< " , ";
	os << "fsid=" << setw(20) << fsId << " , ";
	os << "host=" << setw(32) << host;
	return os;
}

ostream& operator << (ostream &os, const SchedTreeBase::FastTreeInfo &info)
{
	int count = 0;
	for(SchedTreeBase::FastTreeInfo::const_iterator it=info.begin();it!=info.end();it++)
	os << setfill(' ') << "idx=" << setw(4) << count++ << " -> " << (*it) << endl;
	return os;
}

// IMPLEMENTED IN HEADER FILE BECAUSE OF A std::map BUG
//ostream& operator << (ostream &os, const Fs2TreeIdxMap &info) {
//  cout<< "size  is : " << info.size() <<endl;
//  cout<< "begin is : " << &(*(info.begin())) <<endl;
//  cout<< "end   is : " << &(*(info.end())) <<endl;
//  for(Fs2TreeIdxMap::const_iterator it=info.begin();it!=info.end();it++) {
//    cout<< "current   is : " << &(*it) <<endl;
//    //os << setfill(' ')  << "fs=" << setw(20) << it->first << " -> " << "idx=" << (int)it->second << endl;
//  }
//  return os;
//}
EOSMGMNAMESPACE_END
