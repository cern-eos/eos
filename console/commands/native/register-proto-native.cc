// ----------------------------------------------------------------------
// File: register-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <algorithm>
#include <charconv>
#include <limits>
#include <memory>
#include <sstream>

namespace {
std::string
MakeRegisterHelp()
{
  return "Usage: register [-u] <path> [tag1=val1 tag2=val2 ...]\n\n"
         "  -u  update existing file metadata (if file exists)\n\n"
         "Tags: size=100, uid=101|username=foo, gid=102|groupname=bar,\n"
         "  checksum=..., layoutid=..., location=1 location=2 ..., mode=777,\n"
         "  btime=..., atime=..., ctime=..., mtime=..., path=...,\n"
         "  xattr=..., attr=\"sys.acl=u:100:rwx\", atimeifnewer=...\n\n"
         "Tags also accept --tag value. Modes are octal permissions (0..7777).\n"
         "Timestamps are seconds[.nanoseconds]; checksum is at most 64 hex digits.\n"
         "Specify one path and one user/group representation each.\n";
}

bool
ParseUnsigned(const std::string& value, uint64_t& result, uint64_t maximum, int base = 10)
{
  if (value.empty()) {
    return false;
  }
  const auto parsed =
      std::from_chars(value.data(), value.data() + value.size(), result, base);
  return parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size() &&
         result <= maximum;
}

CLI::Validator
UnsignedValue(uint64_t maximum)
{
  return CLI::Validator{[maximum](std::string& value) {
                          uint64_t parsed = 0;
                          if (!ParseUnsigned(value, parsed, maximum)) {
                            return std::string(
                                "expected an unsigned decimal integer in range");
                          }
                          value = std::to_string(parsed);
                          return std::string{};
                        },
                        "UNSIGNED"};
}

const CLI::Validator OwnerValue{
    [](const std::string& value) {
      if (value.empty() || value.front() == '-' || value.front() == '+') {
        return std::string("expected a user/group name or unsigned numeric id");
      }
      if (value.front() >= '0' && value.front() <= '9') {
        uint64_t parsed = 0;
        if (!ParseUnsigned(value, parsed, std::numeric_limits<uint32_t>::max())) {
          return std::string(
              "numeric owner id must be an unsigned 32-bit decimal integer");
        }
      }
      return std::string{};
    },
    "OWNER"};

const CLI::Validator TimestampValue{
    [](const std::string& value) {
      const auto dot = value.find('.');
      uint64_t seconds = 0, nanoseconds = 0;
      if (!ParseUnsigned(value.substr(0, dot), seconds,
                         std::numeric_limits<int64_t>::max()) ||
          (dot != std::string::npos &&
           !ParseUnsigned(value.substr(dot + 1), nanoseconds, 999999999))) {
        return std::string(
            "expected seconds[.nanoseconds] with nanoseconds in [0,999999999]");
      }
      return std::string{};
    },
    "TIMESTAMP"};

struct RegisterArguments {
  bool update = false;
  std::string path, uid, gid, username, groupname, checksum;
  uint64_t size = 0;
  uint32_t mode = 0;
  int32_t layout = 0;
  std::vector<uint32_t> locations;
  std::vector<std::string> attributes;
  std::string btime, ctime, mtime, atime, atime_newer;
};

void
ConfigureRegisterApp(CLI::App& app, RegisterArguments& arguments)
{
  app.name("register");
  app.description("Register a file");
  app.set_help_flag("-h,--help");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeRegisterHelp();
      }));
  app.add_flag("-u", arguments.update, "update existing metadata");
  app.add_option("--path,path", arguments.path, "absolute file path")
      ->required()
      ->check(CLI::Validator{[](const std::string& value) {
                               return !value.empty() && value.front() == '/'
                                          ? std::string{}
                                          : std::string(
                                                "register requires an absolute path");
                             },
                             "ABSOLUTE_PATH"});
  auto* uid = app.add_option("--uid", arguments.uid, "numeric user id or user name")
                  ->check(OwnerValue);
  auto* username = app.add_option("--username", arguments.username, "user name")
                       ->check(eos::console::NonEmptyArgument);
  uid->excludes(username);
  auto* gid = app.add_option("--gid", arguments.gid, "numeric group id or group name")
                  ->check(OwnerValue);
  auto* groupname = app.add_option("--groupname", arguments.groupname, "group name")
                        ->check(eos::console::NonEmptyArgument);
  gid->excludes(groupname);
  app.add_option("--size", arguments.size, "file size")
      ->transform(UnsignedValue(std::numeric_limits<uint64_t>::max()));
  app.add_option("--mode", arguments.mode, "octal permissions")
      ->transform(CLI::Validator{[](std::string& value) {
                                   uint64_t parsed = 0;
                                   if (!ParseUnsigned(value, parsed, 07777, 8)) {
                                     return std::string(
                                         "mode must be octal permissions in [0,7777]");
                                   }
                                   value = std::to_string(parsed);
                                   return std::string{};
                                 },
                                 "OCTAL"});
  app.add_option("--layoutid", arguments.layout, "layout id")
      ->transform(UnsignedValue(std::numeric_limits<int32_t>::max()));
  app.add_option("--location", arguments.locations, "filesystem location")
      ->expected(1)
      ->take_all()
      ->transform(UnsignedValue(std::numeric_limits<uint32_t>::max()));
  app.add_option("--checksum", arguments.checksum, "hexadecimal checksum")
      ->check(CLI::Validator{
          [](const std::string& value) {
            return value.size() <= 64 && value.size() % 2 == 0 &&
                           std::all_of(value.begin(), value.end(),
                                       [](char c) {
                                         return (c >= '0' && c <= '9') ||
                                                (c >= 'a' && c <= 'f') ||
                                                (c >= 'A' && c <= 'F');
                                       })
                       ? std::string{}
                       : std::string(
                             "checksum must contain at most 64 hex digits in byte pairs");
          },
          "HEX_CHECKSUM"});
  app.add_option("--attr,--xattr", arguments.attributes, "extended attribute key=value")
      ->expected(1)
      ->take_all()
      ->check(CLI::Validator{
          [](const std::string& value) {
            const auto separator = value.find('=');
            return separator != std::string::npos && separator != 0
                       ? std::string{}
                       : std::string(
                             "attribute requires a nonempty key followed by =value");
          },
          "KEY=VALUE"});
  app.add_option("--btime", arguments.btime, "birth timestamp")->check(TimestampValue);
  app.add_option("--ctime", arguments.ctime, "change timestamp")->check(TimestampValue);
  app.add_option("--mtime", arguments.mtime, "modification timestamp")
      ->check(TimestampValue);
  auto* atime = app.add_option("--atime", arguments.atime, "access timestamp")
                    ->check(TimestampValue);
  auto* newer = app.add_option("--atimeifnewer", arguments.atime_newer,
                               "update access timestamp only if newer")
                    ->check(TimestampValue);
  atime->excludes(newer);
}

std::vector<std::string>
ExpandRegisterTags(const std::vector<std::string>& args)
{
  std::vector<std::string> expanded;
  bool literal = false;
  bool option_value = false;
  for (const auto& argument : args) {
    if (option_value) {
      expanded.emplace_back(argument);
      option_value = false;
      continue;
    }
    if (argument == "--") {
      literal = true;
    } else if (!literal && argument.rfind("--", 0) == 0 &&
               argument.find('=') == std::string::npos && argument != "--help") {
      option_value = true;
    }
    const auto equals = argument.find('=');
    if (!literal && !argument.empty() && argument.front() != '/' &&
        argument.front() != '-' && equals != std::string::npos) {
      expanded.emplace_back("--" + argument.substr(0, equals));
      expanded.emplace_back(argument.substr(equals + 1));
    } else {
      expanded.emplace_back(argument);
    }
  }
  return expanded;
}

void
SetTimestamp(const std::string& value,
             eos::console::FileRegisterProto::Timestamp* timestamp)
{
  const auto dot = value.find('.');
  uint64_t seconds = 0, nanoseconds = 0;
  ParseUnsigned(value.substr(0, dot), seconds, std::numeric_limits<int64_t>::max());
  if (dot != std::string::npos) {
    ParseUnsigned(value.substr(dot + 1), nanoseconds, 999999999);
  }
  timestamp->set_sec(seconds);
  timestamp->set_nsec(nanoseconds);
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
    std::vector<std::string> arguments;
    return eos::console::TokenizeArguments(arg, arguments) && ParseArguments(arguments);
  }

  bool
  ParseArguments(const std::vector<std::string>& args)
  {
    mReq.clear_record();
    CLI::App app;
    RegisterArguments values;
    ConfigureRegisterApp(app, values);
    if (!eos::console::ParseCliArguments(app, ExpandRegisterTags(args))) {
      return false;
    }
    auto* reg = mReq.mutable_record();
    reg->set_path(values.path);
    reg->set_update(values.update);
    if (*app.get_option("--uid") || *app.get_option("--username")) {
      auto* owner = reg->mutable_owner();
      uint64_t id = 0;
      if (*app.get_option("--uid") &&
          ParseUnsigned(values.uid, id, std::numeric_limits<uint32_t>::max())) {
        owner->set_uid(id);
      } else {
        owner->set_username(*app.get_option("--username") ? values.username : values.uid);
      }
    }
    if (*app.get_option("--gid") || *app.get_option("--groupname")) {
      auto* owner = reg->mutable_owner();
      uint64_t id = 0;
      if (*app.get_option("--gid") &&
          ParseUnsigned(values.gid, id, std::numeric_limits<uint32_t>::max())) {
        owner->set_gid(id);
      } else {
        owner->set_groupname(*app.get_option("--groupname") ? values.groupname
                                                            : values.gid);
      }
    }
    reg->set_size(values.size);
    reg->set_mode(values.mode);
    reg->set_layoutid(values.layout);
    reg->set_checksum(values.checksum);
    for (auto location : values.locations) {
      reg->add_locations(location);
    }
    for (const auto& attribute : values.attributes) {
      const auto separator = attribute.find('=');
      (*reg->mutable_attr())[attribute.substr(0, separator)] =
          attribute.substr(separator + 1);
    }
    if (*app.get_option("--btime")) {
      SetTimestamp(values.btime, reg->mutable_btime());
    }
    if (*app.get_option("--ctime")) {
      SetTimestamp(values.ctime, reg->mutable_ctime());
    }
    if (*app.get_option("--mtime")) {
      SetTimestamp(values.mtime, reg->mutable_mtime());
    }
    if (*app.get_option("--atime") || *app.get_option("--atimeifnewer")) {
      SetTimestamp(*app.get_option("--atime") ? values.atime : values.atime_newer,
                   reg->mutable_atime());
      reg->set_atimeifnewer(app.get_option("--atimeifnewer")->count() != 0);
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
    RegisterArguments values;
    ConfigureRegisterApp(app, values);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterRegisterProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RegisterProtoCommand>());
}
