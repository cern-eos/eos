// ----------------------------------------------------------------------
// File: convert-proto-native.cc
// ----------------------------------------------------------------------

#include "common/LayoutId.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {

class ConvertHelper : public ICmdHelper {
public:
  ConvertHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  ~ConvertHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    using eos::console::ConvertProto_ConfigProto;
    eos::console::ConvertProto* convert = mReq.mutable_convert();
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::string token;
    if (!tokenizer.NextToken(token))
      return false;
    if (token == "config") {
      ConvertProto_ConfigProto* config = convert->mutable_config();
      if (!tokenizer.NextToken(token))
        return false;
      if (token == "list") {
        config->set_op(ConvertProto_ConfigProto::LIST);
      } else if (token == "set") {
        if (!tokenizer.NextToken(token)) {
          return false;
        }
        size_t pos = token.find('=');
        if (pos == std::string::npos || pos == token.length() - 1)
          return false;
        config->set_op(ConvertProto_ConfigProto::SET);
        config->set_key(token.substr(0, pos));
        config->set_value(token.substr(pos + 1));
      } else {
        return false;
      }
    } else if (token == "file") {
      eos::console::ConvertProto_FileProto* file = convert->mutable_file();
      if (!tokenizer.NextToken(token))
        return false;
      file->set_allocated_identifier(ParseIdentifier(token));
      eos::console::ConvertProto_ConversionProto* conversion =
          ParseConversion(tokenizer);
      if (conversion == nullptr)
        return false;
      file->set_allocated_conversion(conversion);
    } else if (token == "rule") {
      if (!tokenizer.NextToken(token))
        return false;
      eos::console::ConvertProto_RuleProto* rule = convert->mutable_rule();
      rule->set_allocated_identifier(ParseIdentifier(token));
      eos::console::ConvertProto_ConversionProto* conversion =
          ParseConversion(tokenizer);
      if (conversion == nullptr)
        return false;
      rule->set_allocated_conversion(conversion);
    } else if (token == "list") {
      convert->mutable_list();
    } else if (token == "clear") {
      convert->mutable_clear();
    } else {
      return false;
    }
    return true;
  }

private:
  eos::console::ConvertProto_IdentifierProto*
  ParseIdentifier(std::string spath)
  {
    XrdOucString path = spath.c_str();
    auto* identifier = new eos::console::ConvertProto_IdentifierProto{};
    unsigned long long id = 0ull;
    if (Path2FileDenominator(path, id))
      identifier->set_fileid(id);
    else if (Path2ContainerDenominator(path, id))
      identifier->set_containerid(id);
    else
      identifier->set_path(abspath(path.c_str()));
    return identifier;
  }

  eos::console::ConvertProto_ConversionProto*
  ParseConversion(eos::common::StringTokenizer& tokenizer)
  {
    std::string token, layout, space, placement, checksum;
    int replica = 0;
    size_t pos;
    bool ok;
    auto validLayout = [](const std::string& l) {
      return eos::common::LayoutId::GetLayoutFromString(l) != -1;
    };
    auto validPlacement = [](const std::string& p) {
      return (p == "scattered" || p == "hybrid" || p == "gathered");
    };
    auto validChecksum = [](const std::string& c) {
      using eos::common::LayoutId;
      auto xs_id = LayoutId::GetChecksumFromString(c);
      return ((xs_id > -1) && (xs_id != LayoutId::eChecksum::kNone));
    };
    if (!tokenizer.NextToken(token))
      return nullptr;
    if ((pos = token.find(":")) == std::string::npos)
      return nullptr;
    layout = token.substr(0, pos);
    try {
      replica = std::stol(token.substr(pos + 1));
    } catch (...) {
      return nullptr;
    }
    if (!validLayout(layout))
      return nullptr;
    if (replica < 1 || replica > 32)
      return nullptr;
    while (tokenizer.NextToken(token)) {
      if ((ok = validChecksum(token)))
        checksum = std::move(token);
      else if ((ok = validPlacement(token)))
        placement = std::move(token);
      else if ((ok = space.empty()))
        space = std::move(token);
      if (!ok)
        return nullptr;
    }
    auto* conversion = new eos::console::ConvertProto_ConversionProto{};
    conversion->set_layout(layout);
    conversion->set_replica(replica);
    conversion->set_space(space);
    conversion->set_placement(placement);
    conversion->set_checksum(checksum);
    return conversion;
  }
};

class ConvertProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "convert";
  }
  const char*
  description() const override
  {
    return "Convert Interface";
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
    ConvertHelper helper(gGlobalOpts);
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
        "Usage: convert <subcomand>                         \n"
        "  convert config list|set [<key>=<value>]          \n"
        "    list: list converter configuration parameters and status\n"
        "    set : set converter configuration parameters. Options:\n"
        "      status               : \"on\" or \"off\"     \n"
        "      max-thread-pool-size : max number of threads in converter pool "
        "[default 100]\n"
        "      max-queue-size       : max number of queued conversion jobs "
        "[default 1000]\n"
        "\n"
        "  convert list                                     \n"
        "    list conversion jobs                           \n"
        "\n"
        "  convert clear                                    \n"
        "    clear list of jobs stored in the backend       \n"
        "\n"
        "  convert file <identifier> <conversion>           \n"
        "    schedule a file conversion                     \n"
        "    <identifier> = fid|fxid|path                   \n"
        "    <conversion> = <layout:replica> [space] [placement] [checksum]\n"
        "\n"
        "  convert rule <identifier> <conversion>           \n"
        "    apply a conversion rule on the given directory \n"
        "    <identifier> = cid|cxid|path                   \n"
        "    <conversion> = <layout:replica> [space] [placement] [checksum]\n");
  }
};
} // namespace

void
RegisterConvertProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ConvertProtoCommand>());
}
