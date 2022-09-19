/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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


#include "benchmark/benchmark.h"
#include "common/StringUtils.hh"
#include <regex>
#include <arpa/inet.h>

using benchmark::Counter;

static void BM_ReMatch(benchmark::State& state)
{
  std::string test = "lxplus8s17.cern.ch";

  for (auto _: state) {
    std::regex lxplus( "(lxplus)(.*)(.cern.ch)");
    benchmark::DoNotOptimize(std::regex_match(test, lxplus));
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_ReMatchPreInit(benchmark::State& state)
{
  std::string test = "lxplus8s17.cern.ch";
  std::regex lxplus( "(lxplus)(.*)(.cern.ch)");

  for (auto _: state) {
    benchmark::DoNotOptimize(std::regex_match(test, lxplus));
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_NoReStartsWith(benchmark::State& state)
{
  std::string test = "lxplus8s17.cern.ch";
  std::string prefix = "lxplus";
  std::string suffix = ".cern.ch";
  for (auto _: state) {
    benchmark::DoNotOptimize(eos::common::startsWith(test, prefix) &&
                             eos::common::endsWith(test, suffix));
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_NoReStartsWithsview(benchmark::State& state)
{
  std::string test = "lxplus8s17.cern.ch";
  constexpr std::string_view prefix = "lxplus";
  constexpr std::string_view suffix = ".cern.ch";
  for (auto _: state) {
    benchmark::DoNotOptimize(eos::common::sview::startsWith(test, prefix) &&
                             eos::common::endsWith(test, suffix));
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_Reipv4(benchmark::State& state)
{
  std::string test =  "188.184.121.11/25";
  for (auto _: state) {
    std::regex ipv4( "([0-9]{1,3}\\.){3}[0-9]{1,3}");
    benchmark::DoNotOptimize(std::regex_match(test, ipv4));
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_Reipv4PreInit(benchmark::State& state)
{
  std::string test =  "188.184.121.11/25";
  std::regex ipv4( "([0-9]{1,3}\\.){3}[0-9]{1,3}");
  for (auto _: state) {
    benchmark::DoNotOptimize(std::regex_match(test, ipv4));
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_NoReipv4(benchmark::State& state)
{
  std::string test = "188.184.121.11/25";
  for (auto _ : state) {
    benchmark::DoNotOptimize(inet_addr(test.c_str()) != INADDR_NONE);
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_NoReipv6(benchmark::State& state)
{
  std::string test = "2001:0db8:85a3:0000:0000:8a2e:0370:7334/64";
  for (auto _ : state) {
    struct in6_addr addr;
    benchmark::DoNotOptimize(inet_pton(AF_INET6, test.c_str(), &addr) == 1);
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

BENCHMARK(BM_ReMatch);
BENCHMARK(BM_ReMatchPreInit);
BENCHMARK(BM_NoReStartsWith);
BENCHMARK(BM_NoReStartsWithsview);
BENCHMARK(BM_Reipv4);
BENCHMARK(BM_Reipv4PreInit);
BENCHMARK(BM_NoReipv4);
BENCHMARK(BM_NoReipv6);
