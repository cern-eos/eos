//------------------------------------------------------------------------------
//! @file Definitions.hh
//! @author Elvin Sindrilaru CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! Extension to the persmission capabilities which are shared between the
//! MGM and the fuse clients
//------------------------------------------------------------------------------
#define D_OK     8 ///< Flag for delete persmission
#define M_OK    16 ///< Flag for chmod permission
#define C_OK    32 ///< Flag for chown permission
#define SA_OK   64 ///< Flag for set xattr permission
#define U_OK   128 ///< Flag for update permission
#define SU_OK  256 ///< Flag for utime permission
#define P_OK   512 ///< Flag for workflow/prepare permission

