//------------------------------------------------------------------------------
// File: BalancerEngineFactory.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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
#include "mgm/groupbalancer/StdDevBalancerEngine.hh"
#include "mgm/groupbalancer/MinMaxBalancerEngine.hh"

namespace eos::mgm::group_balancer {

BalancerEngineT get_engine_type(std::string_view name)
{
  if (name == "minmax") {
    return BalancerEngineT::minmax;
  }
  return BalancerEngineT::stddev;
}

BalancerEngine* make_balancer_engine(BalancerEngineT engine_t)
{
  if (engine_t == BalancerEngineT::minmax) {
    return new MinMaxBalancerEngine();
  }
  return new StdDevBalancerEngine();
}

}
