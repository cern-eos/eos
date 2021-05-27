//------------------------------------------------------------------------------
//! @file PrepareUtils.cc
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "PrepareUtils.hh"
#include <xrootd/XrdVersion.hh>
#include <sstream>
#include <xrootd/XrdSfs/XrdSfsInterface.hh>

EOSMGMNAMESPACE_BEGIN

std::string PrepareUtils::prepareOptsToString(const int opts) {
  std::ostringstream result;
  const int priority = opts & Prep_PMASK;

  switch (priority) {
  case Prep_PRTY0:
    result << "PRTY0";
    break;

  case Prep_PRTY1:
    result << "PRTY1";
    break;

  case Prep_PRTY2:
    result << "PRTY2";
    break;

  case Prep_PRTY3:
    result << "PRTY3";
    break;

  default:
    result << "PRTYUNKNOWN";
  }

  const int send_mask = 12;
  const int send = opts & send_mask;

  switch (send) {
  case 0:
    break;

  case Prep_SENDAOK:
    result << ",SENDAOK";
    break;

  case Prep_SENDERR:
    result << ",SENDERR";
    break;

  case Prep_SENDACK:
    result << ",SENDACK";
    break;

  default:
    result << ",SENDUNKNOWN";
  }

  if (opts & Prep_WMODE) {
    result << ",WMODE";
  }

  if (opts & Prep_STAGE) {
    result << ",STAGE";
  }

  if (opts & Prep_COLOC) {
    result << ",COLOC";
  }

  if (opts & Prep_FRESH) {
    result << ",FRESH";
  }

#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5

  if (opts & Prep_CANCEL) {
    result << ",CANCEL";
  }

  if (opts & Prep_QUERY) {
    result << ",QUERY";
  }

  if (opts & Prep_EVICT) {
    result << ",EVICT";
  }

#endif
  return result.str();
}

EOSMGMNAMESPACE_END