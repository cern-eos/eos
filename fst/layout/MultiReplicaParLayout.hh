//------------------------------------------------------------------------------
//! @file MultiReplicaParLayout.hh
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

#ifndef __EOSFST_MultiReplicaParLayout_HH__
#define __EOSFST_MultiReplicaParLayout_HH__

#include "fst/layout/Layout.hh"

EOSFSTNAMESPACE_BEGIN

class MultiReplicaParLayout : public Layout
{
public:

	virtual int CalculateSpace();
private:
	  int mNumPossibleReplicas; ///< number of possible replicas for the current space, calculate
	  int mNumReplicas; ///< number of replicas for current file
	  bool ioLocal; ///< mark if we are to do local IO
	  bool hasWriteError;
};

EOSFSTNAMESPACE_END

#endif // __EOSFST_MULTIREPLICAPARLAYOUT_HH__
