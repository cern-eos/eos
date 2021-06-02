//------------------------------------------------------------------------------
//! @file PrepareUtils.hh
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

#ifndef EOS_PREPAREUTILS_HH
#define EOS_PREPAREUTILS_HH

#include <string>
#include "mgm/Namespace.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * Utility class for Prepare-related business
 */
class PrepareUtils {
public:
  /**
 * Utility method to convert the prepare options to string options
 * @param opts the prepare options to convert to string
 * @return the prepare options in the string format
 */
  static std::string prepareOptsToString(const int opts);
};

EOSBULKNAMESPACE_END

#endif // EOS_PREPAREUTILS_HH
