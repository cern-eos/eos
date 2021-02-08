//------------------------------------------------------------------------------
//! @file MultiReplicaParLayout.cc
//! @author Andreas Støve - CERN
//! @brief Physical layout of a file with multireplicas
//------------------------------------------------------------------------------

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

#include "fst/layout/MultiReplicaParLayout.hh"
#include "fst/XrdFstOfs.hh"

EOSFSTNAMESPACE_BEGIN

MultiReplicaParLayout::MultiReplicaParLayout(XrdFstOfsFile* file,
											unsigned long lid,
											const XrdSecEntity* client,
											XrdOucErrInfo* outError,
											const char* path,
											uint16_t timeout) :
  Layout(file, lid, client, outError, path, timeout)
{
	mNumReplicas = eos::common::LayoutId::GetStripNumber(lid) + 1;

	ioLocal = false;
	hasWriteErros = false;
}

int MultiReplicaParLayout::CalculateSpace()
{
	int possibleReplicas;
	//Space in total in petabyte
	//Get the space in petabytes from somewhere
	double spaceTotal;
	//Space used in petabytes
	//Get space in petabytes from somewhere
	double spaceUnused;
	//Ratio of used space, for calculating how many replicas there will be
	double ratioOfUsed = spaceUnused/spaceTotal;



	return 0;

}






EOSFSTNAMESPACE_END
