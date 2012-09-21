// ----------------------------------------------------------------------
// File: LinuxMemConsumption.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/ASwitzerland                                  *
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

/**
 * @file   LinuxMemConsumption.hh
 * 
 * @brief  Class measuring the current memory usage using the proc interface
 *  
 */

#ifndef __EOSCOMMON__LINUXMEMCONSUMPTION__HH
#define __EOSCOMMON__LINUXMEMCONSUMPTION__HH

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Static Class to measure memory consumption
//! 
//! Example: linux_mem_t mem; GetMemoryFootprint(mem);
//! 
/*----------------------------------------------------------------------------*/
class LinuxMemConsumption {
public:
  typedef struct {
    unsigned long long vmsize,resident,share,text,lib,data,dt;
  } linux_mem_t;

  static bool GetMemoryFootprint(linux_mem_t& result)
  {
    const char* statm_path = "/proc/self/statm";
    result.vmsize=result.resident=result.share=result.text=result.lib=result.data=result.dt = 0;

    FILE *f = fopen(statm_path,"r");
    if(!f){
      perror(statm_path);
      return false;
    }
    if(7 != fscanf(f,"%lld %lld %lld %lld %lld %lld %lld",
		   &result.vmsize,&result.resident,&result.share,&result.text,&result.lib,&result.data,&result.dt))
      {
	perror(statm_path);
	return false;
      }
    fclose(f);
    // convert into bytes
    result.vmsize*=4096;
    result.resident*=4096;
    result.share*=4096;
    result.lib*=4096;
    result.data*=4096;
    result.dt*=4096;
    result.text*=4096;
    result.lib*=4096;
    result.data*=4096;
    result.dt*=4096;

    return true;
  }
};

EOSCOMMONNAMESPACE_END
 
#endif
