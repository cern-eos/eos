// ----------------------------------------------------------------------
// File: route-proto-native.cc
// ----------------------------------------------------------------------

#include "common/ParseUtils.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
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
      eos::common::StringConversion::Tokenize(soption, endpoints, ",");
      if (endpoints.empty())
        return false;
      for (const auto& endpoint : endpoints) {
        eos::console::RouteProto_LinkProto_Endpoint* ep = link->add_endpoints();
        std::vector<std::string> elems;
        eos::common::StringConversion::Tokenize(endpoint, elems, ":");
        const std::string fqdn = elems[0];
        if (!eos::common::ValidHostnameOrIP(fqdn)) {
          std::cerr << "error: invalid hostname specified" << std::endl;
          return false;
        }
        uint32_t xrd_port = 1094;
        uint32_t http_port = 8000;
        if (elems.size() == 3) {
          try {
            xrd_port = std::stoul(elems[1]);
            http_port = std::stoul(elems[2]);
          } catch (...) {
            std::cerr << "error: failed to parse ports for route" << std::endl;
            return false;
          }
        } else if (elems.size() == 2) {
          try {
            xrd_port = std::stoul(elems[1]);
          } catch (...) {
            std::cerr << "error: failed to parse xrd port for route"
                      << std::endl;
            return false;
          }
        }
        ep->set_fqdn(fqdn);
        ep->set_xrd_port(xrd_port);
        ep->set_http_port(http_port);
      }
    } else {
      return false;
    }
    return true;
  }

private:
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
  run(const std::vector<std::string>& args, CommandContext&) override
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
    RouteHelper helper(gGlobalOpts);
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
    fprintf(
        stderr,
        "Usage: route [ls|link|unlink]\n"
        "    namespace routing to redirect clients to external instances\n"
        "\n"
        "  route ls [<path>]\n"
        "    list all routes or the one matching for the given path\n"
        "      * as the first character means the node is a master\n"
        "      _ as the first character means the node is offline\n"
        "\n"
        "  route link <path> <dst_host>[:<xrd_port>[:<http_port>]],...\n"
        "    create routing from <path> to destination host. If the xrd_port\n"
        "    is omitted the default 1094 is used, if the http_port is omitted\n"
        "    the default 8000 is used. Several dst_hosts can be specified by\n"
        "    separating them with \",\". The redirection will go to the MGM\n"
        "    from the specified list\n"
        "    e.g route /eos/dummy/ foo.bar:1094:8000\n"
        "\n"
        "  route unlink <path>\n"
        "    remove routing matching path\n");
  }
};
} // namespace

void
RegisterRouteProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RouteProtoCommand>());
}
