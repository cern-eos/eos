//------------------------------------------------------------------------------
//! @file MacOSXHelper.hh
//! @author Andreas-Joachim Peters, Geoffray Adde, Elvin Sindrilaru CERN
//! @brief remote IO filesystem implementation
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

#ifndef FUSE_MACOSXHELPER_HH_
#define FUSE_MACOSXHELPER_HH_
#ifdef __APPLE__
#include <pthread.h>
#define MTIMESPEC st_mtimespec
#define ATIMESPEC st_atimespec
#define CTIMESPEC st_ctimespec
#define EBADE 52
#define EBADR 53
#define EADV 68
#define EREMOTEIO 121
#define ENOKEY 126
#else
#define MTIMESPEC st_mtim
#define ATIMESPEC st_atim
#define CTIMESPEC st_ctim
#include <sys/types.h>
#include <signal.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#ifdef __APPLE__
#define thread_id(_x_) (pthread_t) pthread_self()
#else
#define thread_id(_x_) (pthread_t) syscall(SYS_gettid)
#endif

#ifdef __APPLE__
#define thread_alive(_x_) (pthread_kill(_x_,0)==ESRCH)?false:true
#else
#define thread_alive(_x_) (kill( (pid_t)_x_,0))?false:true
#endif

#endif


