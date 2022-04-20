//------------------------------------------------------------------------------
// File: BindArguments.hh
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
#include <tuple>

namespace eos::common {

/*!
 * A simple struct that binds a callable with Arguments so that it may be used
 * in contexts where a function of void(void) signature is necessary, for eg
 * ThreadPools and Executors.
 * The main necessity for doing this instead of a lambda/bind expression is to
 * preserve the c-v-ref nature of arguments in template like scenarios
 * for eg. [&args...](){ my_function(std::forward<Args>(args)...); } will not
 * work correctly as we would end up forwarding a int& where int was expected
 * It is expected to use the bindArgs factory function which correctly creates
 * this structure
 * @tparam Fn Callable
 * @tparam TArgs a tuple of args
 */
template <typename Fn, typename TArgs>
struct BoundArgsHandler {
  Fn f;
  TArgs arg_tuple;

  BoundArgsHandler(Fn&& _f,
                   TArgs&& _arg_tuple) : f(std::move(_f)),
                                         arg_tuple(std::move(_arg_tuple)) {}

  BoundArgsHandler(const Fn& _f,
                   TArgs&& _arg_tuple) : f(_f),
                                         arg_tuple(std::move(_arg_tuple)) {}

  void operator()() const&
  {
    std::apply(f, arg_tuple);
  }

  void operator()() &&
  {
    std::apply(std::move(f), std::move(arg_tuple));
  }
};

template <typename Fn, typename... Args>
auto bindArgs(Fn&& f, Args&&... args) {
  return BoundArgsHandler(std::forward<Fn>(f),
                          std::make_tuple(std::forward<Args>(args)...));
}

} // eos::common
