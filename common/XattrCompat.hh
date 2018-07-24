// ----------------------------------------------------------------------
//! @file: XattrCompat.hh
//! @author: Georgios Bitzes <georgios.bitzes@cern.ch>
// ----------------------------------------------------------------------

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

#pragma once

#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

#if __has_include(<sys/xattr.h>)
#include <sys/xattr.h>
#elif __has_include(<attr/xattr.h>)
#include <attr/xattr.h>
#else
#error "Could not find xattr.h header!"
#endif
