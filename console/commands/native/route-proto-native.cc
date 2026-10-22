// ----------------------------------------------------------------------
// File: route-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <algorithm>
#include <charconv>
#include <cstdint>
#include <memory>

namespace {
struct RouteEndpoint {
  std::string host;
  uint32_t xrdPort = 1094;
  uint32_t httpPort = 8000;
};

bool
ParsePort(const std::string& text, uint32_t& port)
{
  const auto result = std::from_chars(text.data(), text.data() + text.size(), port);
  return result.ec == std::errc{} && result.ptr == text.data() + text.size() &&
         port > 0 && port <= 65535;
}

bool
ValidRouteHost(const std::string& hostname)
{
  std::string host = hostname;
  if (!host.empty() && host.back() == '.') {
    host.pop_back();
  }
  if (host.empty() || host.size() > 253) {
    return false;
  }
  if (host.find('.') != std::string::npos &&
      std::all_of(host.begin(), host.end(), [](unsigned char ch) {
        return (ch >= '0' && ch <= '9') || ch == '.';
      })) {
    size_t begin = 0;
    for (size_t component = 0; component < 4; ++component) {
      const size_t end = host.find('.', begin);
      const std::string part = host.substr(begin, end - begin);
      uint32_t value = 0;
      const auto parsed = std::from_chars(part.data(), part.data() + part.size(), value);
      if (part.empty() || (part.size() > 1 && part.front() == '0') ||
          parsed.ec != std::errc{} || parsed.ptr != part.data() + part.size() ||
          value > 255 || (component < 3 && end == std::string::npos) ||
          (component == 3 && end != std::string::npos)) {
        return false;
      }
      begin = end + 1;
    }
    return true;
  }
  size_t begin = 0;
  while (begin < host.size()) {
    const size_t end = host.find('.', begin);
    const std::string label = host.substr(begin, end - begin);
    if (label.empty() || label.size() > 63 || label.front() == '-' ||
        label.back() == '-' ||
        !std::all_of(label.begin(), label.end(), [](unsigned char ch) {
          return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                 (ch >= '0' && ch <= '9') || ch == '-';
        })) {
      return false;
    }
    if (end == std::string::npos) {
      return true;
    }
    begin = end + 1;
  }
  return false;
}

std::string
ParseEndpoints(const std::string& text, std::vector<RouteEndpoint>& endpoints)
{
  size_t begin = 0;
  do {
    const size_t end = text.find(',', begin);
    const std::string item = text.substr(begin, end - begin);
    const size_t firstColon = item.find(':');
    RouteEndpoint endpoint;
    endpoint.host = item.substr(0, firstColon);
    if (!ValidRouteHost(endpoint.host)) {
      return "invalid destination hostname";
    }
    if (firstColon != std::string::npos) {
      const size_t secondColon = item.find(':', firstColon + 1);
      if (!ParsePort(item.substr(firstColon + 1, secondColon - firstColon - 1),
                     endpoint.xrdPort) ||
          (secondColon != std::string::npos &&
           !ParsePort(item.substr(secondColon + 1), endpoint.httpPort))) {
        return "route ports must be integers between 1 and 65535";
      }
    }
    endpoints.push_back(std::move(endpoint));
    if (end == std::string::npos) {
      return {};
    }
    begin = end + 1;
  } while (begin <= text.size());
  return "destination must not be empty";
}

const CLI::Validator RoutePath{
    [](const std::string& value) {
      if (value.empty() || value.front() != '/') {
        return std::string("path must be absolute and non-empty");
      }
      const std::string path = value.back() == '/' ? value : value + '/';
      if (path.find_first_of(" \t\r\n\v\f\\") != std::string::npos ||
          path.find("/../") != std::string::npos ||
          path.find("/./") != std::string::npos) {
        return std::string(
            "route path must not contain whitespace, backslashes, or dot components");
      }
      return std::string{};
    },
    "ROUTE_PATH"};

struct RouteOptions {
  CLI::App app{"Redirect namespace paths to external instances", "route"};
  CLI::App *ls, *link, *unlink;
  std::string path, destinations;

  RouteOptions()
  {
    app.require_subcommand(1, 1);
    ls = app.add_subcommand("ls", "List all routes or the route matching a path");
    ls->add_option("path", path)->check(RoutePath);
    link = app.add_subcommand("link", "Create a route to one or more destination hosts");
    link->add_option("path", path)->required()->check(RoutePath);
    link->add_option("destinations", destinations,
                     "host[:xrd_port[:http_port]],... (defaults: 1094 and 8000)")
        ->required()
        ->check(CLI::Validator(
            [](const std::string& value) {
              std::vector<RouteEndpoint> parsed;
              return ParseEndpoints(value, parsed);
            },
            "HOST[:PORT[:PORT]],..."));
    unlink = app.add_subcommand("unlink", "Remove the route matching a path");
    unlink->add_option("path", path)->required()->check(RoutePath);
  }
};

class RouteHelper : public ICmdHelper {
public:
  explicit RouteHelper(const GlobalOptions& opts)
      : ICmdHelper(opts)
  {
  }
  bool
  ParseCommand(const char* arg) override
  {
    std::vector<std::string> args;
    return eos::console::TokenizeArguments(arg, args) && ParseArguments(args);
  }
  bool
  ParseArguments(const std::vector<std::string>& args)
  {
    RouteOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      return false;
    }
    if (!options.path.empty() && options.path.back() != '/') {
      options.path += '/';
    }
    auto* route = mReq.mutable_route();
    if (*options.ls) {
      route->mutable_list()->set_path(options.path);
    } else if (*options.unlink) {
      route->mutable_unlink()->set_path(options.path);
    } else if (*options.link) {
      auto* link = route->mutable_link();
      link->set_path(options.path);
      std::vector<RouteEndpoint> endpoints;
      if (!ParseEndpoints(options.destinations, endpoints).empty()) {
        return false;
      }
      for (const auto& endpoint : endpoints) {
        auto* destination = link->add_endpoints();
        destination->set_fqdn(endpoint.host);
        destination->set_xrd_port(endpoint.xrdPort);
        destination->set_http_port(endpoint.httpPort);
      }
    }
    return true;
  }
};

class RouteProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "route";
  }
  const char*
  description() const override
  {
    return "Routing interface";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    RouteHelper helper(*ctx.globalOpts);
    if (!helper.ParseArguments(args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  std::string
  helpText() const override
  {
    return RouteOptions{}.app.help();
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", helpText().c_str());
  }
};
} // namespace

void
RegisterRouteProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RouteProtoCommand>());
}
