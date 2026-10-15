// ----------------------------------------------------------------------
// File: register-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "common/Timing.hh"
#include "console/CommandFramework.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>

namespace {
std::string MakeRegisterHelp()
{
  return "Usage: register [-u] <path> [tag1=val1 tag2=val2 ...]\n\n"
         "  -u  update existing file metadata (if file exists)\n\n"
         "Tags: size=100, uid=101|username=foo, gid=102|groupname=bar,\n"
         "  checksum=..., layoutid=..., location=1 location=2 ..., mode=777,\n"
         "  btime=..., atime=..., ctime=..., mtime=..., path=...,\n"
         "  xattr=..., attr=\"sys.acl=u:100:rwx\", atimeifnewer=...\n";
}

void ConfigureRegisterApp(CLI::App& app)
{
  app.name("register");
  app.description("Register a file");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeRegisterHelp();
      }));
}

uint64_t
ParseUnsigned(const std::string& value, uint64_t maximum, int base = 10)
{
  if (value.empty() || !std::all_of(value.begin(), value.end(), [base](char c) {
        return c >= '0' && c < '0' + base;
      })) {
    throw std::invalid_argument("invalid unsigned integer");
  }
  const auto parsed = std::stoull(value, nullptr, base);
  if (parsed > maximum) {
    throw std::out_of_range("integer out of range");
  }
  return parsed;
}

class RegisterHelper : public ICmdHelper {
public:
  explicit RegisterHelper(const GlobalOptions& opts)
      : ICmdHelper(opts)
  {
    mIsAdmin = true;
  }
  bool
  ParseCommand(const char* arg) override
  {
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::vector<std::string> args;
    while (const char* token = tokenizer.GetTokenUnquoted(false)) {
      args.emplace_back(token);
    }
    return ParseArguments(args);
  }
  bool
  ParseArguments(const std::vector<std::string>& args)
  {
    mReq.clear_record();
    auto* reg = mReq.mutable_record();
    try {
      for (const auto& argument : args) {
        if (argument == "-u") {
          reg->set_update(true);
          continue;
        }
        const auto eq = !argument.empty() && argument.front() == '/' ? std::string::npos
                                                                     : argument.find('=');
        const auto key = argument.substr(0, eq);
        const auto value = eq == std::string::npos ? argument : argument.substr(eq + 1);
        if (eq == std::string::npos || key == "path") {
          if (value.empty() || value.front() != '/') {
            throw std::invalid_argument("register requires an absolute path");
          }
          reg->set_path(value);
        } else if (key == "uid" || key == "gid" || key == "username" ||
                   key == "groupname") {
          if (value.empty()) {
            throw std::invalid_argument("empty owner");
          }
          const bool numeric = eos::common::StringConversion::IsDecimalNumber(value);
          if (key == "username" || (key == "uid" && !numeric)) {
            reg->mutable_owner()->set_username(value);
          } else if (key == "groupname" || (key == "gid" && !numeric)) {
            reg->mutable_owner()->set_groupname(value);
          } else {
            const auto id = ParseUnsigned(value, std::numeric_limits<uint32_t>::max());
            if (key == "uid") {
              reg->mutable_owner()->set_uid(id);
            } else {
              reg->mutable_owner()->set_gid(id);
            }
          }
        } else if (key == "size") {
          reg->set_size(ParseUnsigned(value, std::numeric_limits<uint64_t>::max()));
        } else if (key == "mode") {
          reg->set_mode(ParseUnsigned(value, std::numeric_limits<int64_t>::max(), 8));
        } else if (key == "layoutid") {
          reg->set_layoutid(ParseUnsigned(value, std::numeric_limits<int32_t>::max()));
        } else if (key == "location") {
          reg->add_locations(ParseUnsigned(value, std::numeric_limits<uint32_t>::max()));
        } else if (key == "checksum") {
          reg->set_checksum(value);
        } else if (key == "attr" || key == "xattr") {
          const auto separator = value.find('=');
          if (separator == std::string::npos || separator == 0) {
            throw std::invalid_argument("attribute requires key=value");
          }
          (*reg->mutable_attr())[value.substr(0, separator)] =
              value.substr(separator + 1);
        } else if (key == "ctime" || key == "mtime" || key == "btime" || key == "atime" ||
                   key == "atimeifnewer") {
          const auto dot = value.find('.');
          ParseUnsigned(value.substr(0, dot), std::numeric_limits<int64_t>::max());
          if (dot != std::string::npos) {
            ParseUnsigned(value.substr(dot + 1), 999999999);
          }
          struct timespec ts;
          if (eos::common::Timing::Timespec_from_TimespecStr(value, ts)) {
            throw std::invalid_argument("invalid timestamp");
          }
          auto* timestamp = key == "ctime"   ? reg->mutable_ctime()
                            : key == "mtime" ? reg->mutable_mtime()
                            : key == "btime" ? reg->mutable_btime()
                                             : reg->mutable_atime();
          timestamp->set_sec(ts.tv_sec);
          timestamp->set_nsec(ts.tv_nsec);
          if (key == "atimeifnewer") {
            reg->set_atimeifnewer(true);
          }
        } else {
          throw std::invalid_argument("unknown register tag");
        }
      }
      if (reg->path().empty()) {
        throw std::invalid_argument("missing path");
      }
    } catch (const std::exception&) {
      return false;
    }
    return true;
  }
};

class RegisterProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "register";
  }
  const char*
  description() const override
  {
    return "Register a file";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    RegisterHelper helper(*ctx.globalOpts);
    if (!helper.ParseArguments(args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute(true, true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureRegisterApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterRegisterProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RegisterProtoCommand>());
}
