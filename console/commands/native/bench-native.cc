// ----------------------------------------------------------------------
// File: bench-native.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleCommandHelp.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace
{

std::atomic<bool> gBenchInterrupted{false};

void
BenchSigHandler(int)
{
  gBenchInterrupted = true;
}

int
RunCmd(const std::string& name, const std::vector<std::string>& args)
{
  IConsoleCommand* cmd = CommandRegistry::instance().find(name);

  if (!cmd) {
    fprintf(stderr, "error: command '%s' not available\n", name.c_str());
    return EINVAL;
  }

  CommandContext ctx;
  ctx.globalOpts = &gGlobalOpts;
  ctx.json = json;
  ctx.silent = silent;
  ctx.clientCommand = &client_command;
  ctx.outputResult = &output_result;
  return cmd->run(args, ctx);
}

class BenchHelper : public ICmdHelper
{
public:
  explicit BenchHelper(const GlobalOptions& opts) : ICmdHelper(opts)
  {
    mIsAdmin = true;
  }

  bool
  ParseCommand(const char* arg) override
  {
    // Unused — callers fill mReq directly.
    (void) arg;
    return true;
  }

  eos::console::RequestProto&
  Req()
  {
    return mReq;
  }
};

struct NodeSample {
  std::string endpoint;
  std::string phase;
  bool running = false;
  uint64_t bytes_out = 0;
  uint64_t bytes_in = 0;
  uint64_t errors = 0;
  uint64_t rate_out = 0;
  uint64_t rate_in = 0;
};

uint64_t
ParseKeyU64(const std::string& line, const std::string& key)
{
  const std::string token = key + "=";
  auto pos = line.find(token);

  if (pos == std::string::npos) {
    return 0;
  }

  pos += token.size();
  auto end = line.find_first_of(" \t\n", pos);
  const std::string val = line.substr(pos, end - pos);

  try {
    return std::stoull(val);
  } catch (...) {
    return 0;
  }
}

std::string
ParseKeyStr(const std::string& line, const std::string& key)
{
  const std::string token = key + "=";
  auto pos = line.find(token);

  if (pos == std::string::npos) {
    return {};
  }

  pos += token.size();
  auto end = line.find_first_of(" \t\n", pos);
  return line.substr(pos, end - pos);
}

std::vector<NodeSample>
ParseStatusOutput(const std::string& text)
{
  std::vector<NodeSample> out;
  std::istringstream iss(text);
  std::string line;

  while (std::getline(iss, line)) {
    if (line.find("endpoint=") == std::string::npos) {
      continue;
    }

    NodeSample s;
    s.endpoint = ParseKeyStr(line, "endpoint");
    s.phase = ParseKeyStr(line, "phase");
    s.running = ParseKeyU64(line, "running") != 0;
    s.bytes_out = ParseKeyU64(line, "bytes_out");
    s.bytes_in = ParseKeyU64(line, "bytes_in");
    s.errors = ParseKeyU64(line, "err");
    s.rate_out = ParseKeyU64(line, "rate_out");
    s.rate_in = ParseKeyU64(line, "rate_in");
    out.push_back(std::move(s));
  }

  return out;
}

bool
ParseLayout(const std::string& layout, std::string& name, std::string& stripes)
{
  auto pos = layout.find(':');

  if (pos == std::string::npos || pos == 0 || pos + 1 >= layout.size()) {
    return false;
  }

  name = layout.substr(0, pos);
  stripes = layout.substr(pos + 1);
  return !name.empty() && !stripes.empty();
}

std::string
MakeBenchHelp()
{
  std::ostringstream oss;
  oss << "Usage: bench --syntheticio --layout <name>:<stripes> "
         "<duration> <parallelism>\n\n"
      << "  Instance bandwidth benchmark using SyntheticIO clients that run\n"
      << "  on every online FST node (not on the MGM/admin host).\n\n"
      << "  bench --syntheticio --layout <name>:<stripes> <duration> <parallelism>\n"
      << "    Run SyntheticIO uploads then downloads from each online FST.\n\n"
      << "  --syntheticio              enable SyntheticIO FST client mode\n"
      << "  --layout <name>:<stripes>  e.g. replica:2 or raid6:6\n"
      << "  <duration>                 seconds per phase (write then read)\n"
      << "  <parallelism>              streams originating from each FST\n\n"
      << "  Prepares /network with sys.forced.syntheticio and the requested\n"
      << "  layout, starts FST clients via the MGM, prints dstat-style rates,\n"
      << "  then cleans up files under /network/.\n";
  return oss.str();
}

class BenchCommand : public IConsoleCommand
{
public:
  const char*
  name() const override
  {
    return "bench";
  }

  const char*
  description() const override
  {
    return "Instance SyntheticIO bandwidth benchmark";
  }

  std::string
  helpText() const override
  {
    return MakeBenchHelp();
  }

  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }

  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::ostringstream joined_oss;

    for (size_t i = 0; i < args.size(); ++i) {
      if (i) {
        joined_oss << ' ';
      }

      joined_oss << args[i];
    }

    const std::string joined = joined_oss.str();

    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    bool syntheticio = false;
    std::string layout;
    std::vector<std::string> positional;

    for (size_t i = 0; i < args.size(); ++i) {
      if (args[i] == "--syntheticio") {
        syntheticio = true;
      } else if (args[i] == "--layout") {
        if (i + 1 >= args.size()) {
          fprintf(stderr, "error: --layout requires name:stripes\n");
          global_retc = EINVAL;
          return 0;
        }

        layout = args[++i];
      } else if (args[i].rfind("--", 0) == 0) {
        fprintf(stderr, "error: unknown option '%s'\n", args[i].c_str());
        printHelp();
        global_retc = EINVAL;
        return 0;
      } else {
        positional.push_back(args[i]);
      }
    }

    if (!syntheticio) {
      fprintf(stderr, "error: only --syntheticio mode is implemented\n");
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    if (layout.empty() || positional.size() != 2) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::string layout_name;
    std::string layout_stripes;

    if (!ParseLayout(layout, layout_name, layout_stripes)) {
      fprintf(stderr, "error: invalid --layout '%s' (expected name:stripes)\n",
              layout.c_str());
      global_retc = EINVAL;
      return 0;
    }

    uint32_t duration = 0;
    uint32_t parallel = 0;

    try {
      duration = static_cast<uint32_t>(std::stoul(positional[0]));
      parallel = static_cast<uint32_t>(std::stoul(positional[1]));
    } catch (...) {
      fprintf(stderr, "error: duration and parallelism must be integers\n");
      global_retc = EINVAL;
      return 0;
    }

    if (duration == 0 || parallel == 0) {
      fprintf(stderr, "error: duration and parallelism must be > 0\n");
      global_retc = EINVAL;
      return 0;
    }

    const std::string path = "/network";
    int rc = 0;
    rc |= RunCmd("mkdir", {"-p", path});
    rc |= RunCmd("attr", {"set", "sys.forced.syntheticio=algorithm:deterministic",
                          path});
    rc |= RunCmd("attr", {"set", "sys.forced.layout=" + layout_name, path});
    rc |= RunCmd("attr", {"set", "sys.forced.nstripes=" + layout_stripes, path});

    if (rc) {
      fprintf(stderr, "error: failed to prepare %s\n", path.c_str());
      global_retc = rc;
      return 0;
    }

    struct sigaction sa {};
    sa.sa_handler = BenchSigHandler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    BenchHelper starter(gGlobalOpts);
    auto* start = starter.Req().mutable_bench()->mutable_start();
    start->set_duration(duration);
    start->set_parallel(parallel);
    start->set_size(1ull << 30);
    start->set_path(path);
    start->set_nodename("*");
    global_retc = starter.ExecuteWithoutPrint();

    if (global_retc) {
      fprintf(stderr, "error: failed to start FST bench: %s\n",
              starter.GetError().c_str());
      return 0;
    }

    fprintf(stdout, "# prepared %s layout=%s:%s duration=%u parallel/FST=%u\n",
            path.c_str(), layout_name.c_str(), layout_stripes.c_str(), duration,
            parallel);
    fprintf(stdout, "%s\n", starter.GetResult().c_str());
    fflush(stdout);

    const auto t0 = std::chrono::steady_clock::now();
    bool printed_write = false;
    bool printed_read = false;
    uint64_t peak_write = 0;
    uint64_t peak_read = 0;
    uint64_t last_bytes_out = 0;
    uint64_t last_bytes_in = 0;
    uint64_t total_err = 0;

    auto stop_all = [&]() {
      BenchHelper stopper(gGlobalOpts);
      stopper.Req().mutable_bench()->mutable_stop()->set_nodename("*");
      (void) stopper.ExecuteWithoutPrint();
    };

    while (!gBenchInterrupted) {
      BenchHelper statuser(gGlobalOpts);
      statuser.Req().mutable_bench()->mutable_status()->set_nodename("*");
      const int st_rc = statuser.ExecuteWithoutPrint();
      const auto samples = ParseStatusOutput(statuser.GetResult());

      if (st_rc && samples.empty()) {
        fprintf(stderr, "error: status failed: %s\n", statuser.GetError().c_str());
        stop_all();
        global_retc = st_rc;
        break;
      }

      uint64_t sum_out = 0;
      uint64_t sum_in = 0;
      uint64_t sum_rate_out = 0;
      uint64_t sum_rate_in = 0;
      uint64_t sum_err = 0;
      uint32_t n_running = 0;
      uint32_t n_nodes = static_cast<uint32_t>(samples.size());
      std::string phase = "idle";

      for (const auto& s : samples) {
        sum_out += s.bytes_out;
        sum_in += s.bytes_in;
        sum_rate_out += s.rate_out;
        sum_rate_in += s.rate_in;
        sum_err += s.errors;

        if (s.running) {
          ++n_running;
        }

        if (!s.phase.empty() && s.phase != "idle" && s.phase != "done") {
          phase = s.phase;
        } else if (phase == "idle" && !s.phase.empty()) {
          phase = s.phase;
        }
      }

      total_err = sum_err;
      const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
          .count();

      if (phase == "write" && !printed_write) {
        fprintf(stdout, "---- syntheticio write ----\n");
        fprintf(stdout, "# %8s %6s %8s %10s %10s %6s\n", "time", "nodes",
                "streams", "MB/s", "GB_tot", "err");
        printed_write = true;
      } else if (phase == "read" && !printed_read) {
        fprintf(stdout, "---- syntheticio read -----\n");
        fprintf(stdout, "# %8s %6s %8s %10s %10s %6s\n", "time", "nodes",
                "streams", "MB/s", "GB_tot", "err");
        printed_read = true;
      }

      const uint64_t rate =
        (phase == "read") ? sum_rate_in : sum_rate_out;
      const uint64_t bytes =
        (phase == "read") ? sum_in : sum_out;
      const double mbs = static_cast<double>(rate) / (1024.0 * 1024.0);
      const double gbtot = static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);

      if (phase == "write" || phase == "read") {
        fprintf(stdout, "  %7.1fs %6u %8u %10.1f %10.2f %6llu\n", elapsed,
                n_nodes, n_nodes * parallel, mbs, gbtot,
                static_cast<unsigned long long>(sum_err));
        fflush(stdout);

        if (phase == "write" && rate > peak_write) {
          peak_write = rate;
        }

        if (phase == "read" && rate > peak_read) {
          peak_read = rate;
        }
      }

      last_bytes_out = sum_out;
      last_bytes_in = sum_in;

      if (n_running == 0 && (phase == "done" || phase == "idle") &&
          printed_write) {
        break;
      }

      // Safety: write+read + small margin
      if (elapsed > (2.0 * duration + 30.0)) {
        fprintf(stderr, "warning: bench watchdog timeout, stopping\n");
        stop_all();
        break;
      }

      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (gBenchInterrupted) {
      fprintf(stderr, "info: interrupted, stopping FST clients\n");
      stop_all();
      global_retc = EINTR;
    }

    const double write_gb =
      static_cast<double>(last_bytes_out) / (1024.0 * 1024.0 * 1024.0);
    const double read_gb =
      static_cast<double>(last_bytes_in) / (1024.0 * 1024.0 * 1024.0);
    fprintf(stdout, "---- summary ----\n");
    fprintf(stdout,
            "write: %.2f GB  peak %.1f MB/s  (aggregate across FST clients)\n",
            write_gb, peak_write / (1024.0 * 1024.0));
    fprintf(stdout,
            "read:  %.2f GB  peak %.1f MB/s  (aggregate across FST clients)\n",
            read_gb, peak_read / (1024.0 * 1024.0));
    fprintf(stdout, "errors: %llu\n",
            static_cast<unsigned long long>(total_err));
    fflush(stdout);

    // Best-effort cleanup; next run recreates /network and attrs.
    (void) RunCmd("rm", {"-r", "--no-confirmation", path});
    return 0;
  }

  void
  printHelp() const override
  {
    eos::console::PrintLegacyHelp(MakeBenchHelp());
  }
};

} // namespace

void
RegisterBenchNativeCommand()
{
  eos::console::CommandHelpSpec help;
  help.topics = {
      {{"--syntheticio"},
       "  bench --syntheticio --layout ",
       "Run SyntheticIO instance bandwidth bench from FSTs."},
  };
  help.valueOptions = {{"--layout", 1}};
  CommandRegistry::instance().reg(
      eos::console::WithCommandHelp(std::make_unique<BenchCommand>(),
                                    std::move(help)));
}
