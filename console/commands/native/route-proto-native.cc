// ----------------------------------------------------------------------
// File: route-proto-native.cc
// ----------------------------------------------------------------------

#include "common/ParseUtils.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <CLI/CLI.hpp>
#include <charconv>
#include <iomanip>
#include <memory>
#include <sstream>

namespace {
std::string MakeRouteHelp()
{
  std::ostringstream oss;
  oss << "Usage: route [ls|link|unlink]" << std::endl
      << "    namespace routing to redirect clients to external instances"
      << std::endl
      << std::endl
      << "  route ls [<path>]" << std::endl
      << "    list all routes or the one matching for the given path"
      << std::endl
      << "      * as the first character means the node is a master"
      << std::endl
      << "      _ as the first character means the node is offline"
      << std::endl
      << std::endl
      << "  route link <path> <dst_host>[:<xrd_port>[:<http_port>]],..."
      << std::endl
      << "    create routing from <path> to destination host. If the xrd_port"
      << std::endl
      << "    is omitted the default 1094 is used, if the http_port is omitted"
      << std::endl
      << "    the default 8000 is used. Several dst_hosts can be specified by"
      << std::endl
      << "    separating them with \",\". The redirection will go to the MGM"
      << std::endl
      << "    from the specified list"
      << std::endl
      << "    e.g route /eos/dummy/ foo.bar:1094:8000" << std::endl
      << std::endl
      << "  route unlink <path>" << std::endl
      << "    remove routing matching path" << std::endl;
  return oss.str();
}

void ConfigureRouteApp(CLI::App& app)
{
  app.name("route");
  app.description("Routing interface");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeRouteHelp();
      }));
}

// Ported from legacy com_proto_route.cc
class RouteHelper : public ICmdHelper {
public:
  RouteHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  ~RouteHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    const char* option;
    std::string soption;
    eos::console::RouteProto* route = mReq.mutable_route();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    option = tokenizer.GetToken();
    std::string cmd = (option ? option : "");
    if (cmd == "ls") {
      using eos::console::RouteProto_ListProto;
      RouteProto_ListProto* list = route->mutable_list();
      if (!(option = tokenizer.GetToken())) {
        list->set_path("");
      } else {
        soption = option;
        if (!ValidatePath(soption))
          return false;
        list->set_path(soption);
      }
    } else if (cmd == "unlink") {
      using eos::console::RouteProto_UnlinkProto;
      RouteProto_UnlinkProto* unlink = route->mutable_unlink();
      if (!(option = tokenizer.GetToken()))
        return false;
      soption = option;
      if (!ValidatePath(soption))
        return false;
      unlink->set_path(soption);
    } else if (cmd == "link") {
      if (!(option = tokenizer.GetToken()))
        return false;
      soption = option;
      if (!ValidatePath(soption))
        return false;
      eos::console::RouteProto_LinkProto* link = route->mutable_link();
      link->set_path(soption);
      if (!(option = tokenizer.GetToken()))
        return false;
      soption = option;
      std::vector<std::string> endpoints;
      eos::common::StringConversion::EmptyTokenize(soption, endpoints, ",");
      if (endpoints.empty() || soption.front() == ',') {
        return false;
      }
      for (const auto& endpoint : endpoints) {
        std::vector<std::string> elems;
        eos::common::StringConversion::EmptyTokenize(endpoint, elems, ":");
        if (elems.empty() || elems.size() > 3 || endpoint.front() == ':' ||
            elems[0].empty()) {
          return false;
        }
        const std::string& fqdn = elems[0];
        if (!eos::common::ValidHostnameOrIP(fqdn)) {
          std::cerr << "error: invalid hostname specified" << std::endl;
          return false;
        }
        uint32_t xrd_port = 1094;
        uint32_t http_port = 8000;
        if ((elems.size() >= 2 && !ParsePort(elems[1], xrd_port)) ||
            (elems.size() == 3 && !ParsePort(elems[2], http_port))) {
          std::cerr << "error: route ports must be integers between 1 and 65535"
                    << std::endl;
          return false;
        }
        auto* ep = link->add_endpoints();
        ep->set_fqdn(fqdn);
        ep->set_xrd_port(xrd_port);
        ep->set_http_port(http_port);
      }
    } else {
      return false;
    }
    return tokenizer.GetToken() == nullptr;
  }

private:
  static bool
  ParsePort(const std::string& text, uint32_t& port)
  {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), port);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size() &&
           port > 0 && port <= 65535;
  }

  bool
  ValidatePath(std::string& path) const
  {
    if (path.empty() || path[0] != '/') {
      std::cerr << "error: path should be non-empty and start with '/'"
                << std::endl;
      return false;
    }
    if (path.back() != '/') {
      path += '/';
    }
    std::set<std::string> forbidden{" ", "/../", "/./", "\\"};
    for (const auto& needle : forbidden) {
      if (path.find(needle) != std::string::npos) {
        std::cerr
            << "error: path should no contain any of the following sequences "
               "of characters: \" \", \"/../\", \"/./\" or \"\\\""
            << std::endl;
        return false;
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
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      if (args[i].find(' ') != std::string::npos)
        oss << std::quoted(args[i]);
      else
        oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    RouteHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureRouteApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterRouteProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RouteProtoCommand>());
}
