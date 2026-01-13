// ----------------------------------------------------------------------
// File: scitoken-native.cc
// ----------------------------------------------------------------------

// ----------------------------------------------------------------------
// File: scitoken-native.cc
// ----------------------------------------------------------------------

#include "common/Mapping.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <json/json.h>
#include <memory>
#include <sstream>
#ifndef __APPLE__
#include "console/commands/helpers/jwk_generator/jwk_generator.hpp"
#include <fstream>
#include <scitokens/scitokens.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {
class ScitokenCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "scitoken";
  }
  const char*
  description() const override
  {
    return "SciToken utilities";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
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
#ifdef __APPLE__
    fprintf(stderr, "error: scitoken command is not support on OSX\n");
    global_retc = EINVAL;
    return 0;
#else
    eos::common::StringTokenizer subtokenizer(joined.c_str());
    subtokenizer.GetLine();
    XrdOucString subcommand = subtokenizer.GetTokenUnquoted();
    std::string option;
    std::string value;
    time_t expires = 0;
    std::string cred, key, creddata, keydata, keyid, issuer, profile = "wlcg";
    std::set<std::string> claims;
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    if (subcommand == "create") {
      do {
        const char* o = subtokenizer.GetTokenUnquoted();
        const char* v = subtokenizer.GetTokenUnquoted();
        if (o && !v) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        if (!o && !v)
          break;
        option = o;
        value = v;
        if (option == "--pubkey")
          cred = value;
        if (option == "--privkey")
          key = value;
        if (option == "--keyid")
          keyid = value;
        if (option == "--issuer")
          issuer = value;
        if (option == "--claim")
          claims.insert(value);
        if (option == "--expires")
          expires = atoi(value.c_str());
        if (option == "--profile")
          profile = value;
      } while (option.length());
      if (issuer.empty() || !claims.size() || keyid.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      if (cred.empty()) {
        cred = "/etc/xrootd/" + keyid + std::string("-pkey.pem");
      }
      if (key.empty()) {
        key = "/etc/xrootd/" + keyid + std::string("-key.pem");
      }
      eos::common::StringConversion::LoadFileIntoString(key.c_str(), keydata);
      if (keydata.empty()) {
        std::cerr << "error: cannot load private key from '" << key.c_str()
                  << "'" << std::endl;
        global_retc = EINVAL;
        return 0;
      }
      eos::common::StringConversion::LoadFileIntoString(cred.c_str(), creddata);
      if (creddata.empty()) {
        std::cerr << "error: cannot load public key from '" << cred.c_str()
                  << "'" << std::endl;
        global_retc = EINVAL;
        return 0;
      }
      char* err_msg = 0;
      auto key_raw = scitoken_key_create(
          keyid.c_str(), "ES256", creddata.c_str(), keydata.c_str(), &err_msg);
      std::unique_ptr<void, decltype(&scitoken_key_destroy)> pkey(
          key_raw, scitoken_key_destroy);
      if (key_raw == nullptr) {
        std::cerr << "error: failed to generate a key: " << err_msg
                  << std::endl;
        free(err_msg);
        global_retc = EFAULT;
        return 0;
      }
      std::unique_ptr<void, decltype(&scitoken_destroy)> token(
          scitoken_create(key_raw), scitoken_destroy);
      if (token.get() == nullptr) {
        std::cerr << "error: failed to generate a new token" << std::endl;
        global_retc = EFAULT;
        return 0;
      }
      int rv = scitoken_set_claim_string(token.get(), "iss", issuer.c_str(),
                                         &err_msg);
      if (rv) {
        std::cerr << "error: failed to set issuer: " << err_msg << std::endl;
        free(err_msg);
        global_retc = EFAULT;
        return 0;
      }
      for (const auto& c : claims) {
        auto pos = c.find("=");
        if (pos == std::string::npos) {
          std::cerr << "error: claim must contain a '=' character: "
                    << c.c_str() << std::endl;
          global_retc = EFAULT;
          return 0;
        }
        auto k = c.substr(0, pos);
        auto val = c.substr(pos + 1);
        rv = scitoken_set_claim_string(token.get(), k.c_str(), val.c_str(),
                                       &err_msg);
        if (rv) {
          std::cerr << "error: failed to set claim '" << k << "'='" << val
                    << "' error:" << err_msg << std::endl;
          free(err_msg);
          global_retc = EFAULT;
          return 0;
        }
      }
      if (expires) {
        auto lifetime = expires - time(NULL);
        if (lifetime < 0)
          lifetime = 0;
        scitoken_set_lifetime(token.get(), lifetime);
      }
      SciTokenProfile sprofile = WLCG_1_0;
      if (profile == "wlcg")
        sprofile = WLCG_1_0;
      else if (profile == "scitokens1")
        sprofile = SCITOKENS_1_0;
      else if (profile == "scitokens2")
        sprofile = SCITOKENS_2_0;
      else if (profile == "atjwt")
        sprofile = AT_JWT;
      else {
        std::cerr << "error: unknown token profile: " << profile << std::endl;
        global_retc = EINVAL;
        return 0;
      }
      scitoken_set_serialize_mode(token.get(), sprofile);
      char* out = 0;
      rv = scitoken_serialize(token.get(), &out, &err_msg);
      if (rv) {
        std::cerr << "error: failed to serialize the token: " << err_msg
                  << std::endl;
        free(err_msg);
        global_retc = EFAULT;
        return 0;
      }
      std::cout << out << std::endl;
      return 0;
    }
    if (subcommand == "dump") {
      XrdOucString token = subtokenizer.GetTokenUnquoted();
      if (!token.length()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      try {
        std::string stoken = token.c_str();
        std::cerr
            << "# -----------------------------------------------------------"
               "-------------------- #"
            << std::endl;
        std::cerr << eos::common::Mapping::PrintJWT(stoken, false) << std::endl;
        std::cerr
            << "# -----------------------------------------------------------"
               "-------------------- #"
            << std::endl;
        global_retc = 0;
      } catch (...) {
        std::cerr << "error: failed to print token" << std::endl;
        global_retc = EINVAL;
      }
      return 0;
    }
    if (subcommand == "create-keys") {
      std::string option, value, keyid;
      do {
        const char* o = subtokenizer.GetTokenUnquoted();
        const char* v = subtokenizer.GetTokenUnquoted();
        if (o && !v) {
          printHelp();
          global_retc = EINVAL;
          return 0;
        }
        if (!o && !v)
          break;
        option = o;
        value = v;
        if (option == "--keyid")
          keyid = value;
      } while (option.length());
      std::string prefix;
      if (!keyid.empty()) {
        prefix = "/etc/xrootd/";
      } else {
        keyid = "default";
        const size_t size = 1024;
        char buffer[size];
        if (getcwd(buffer, size) == nullptr) {
          std::cerr << "error: can not get CWD" << std::endl;
          global_retc = errno;
          return 0;
        }
        prefix = buffer;
      }
      if (!prefix.empty() && prefix.back() != '/')
        prefix += '/';
      bool store_keys = false;
      std::string fn_public = prefix + keyid + "-pkey.pem";
      std::string fn_private = prefix + keyid + "-key.pem";
      std::string jwk_file;
      struct stat buf;
      if (::stat(fn_public.c_str(), &buf) || ::stat(fn_private.c_str(), &buf)) {
        fn_public.clear();
        fn_private.clear();
        store_keys = true;
      }
      using namespace jwk_generator;
      JwkGenerator<ES256> jwk(keyid, fn_public, fn_private);
      std::cout << "JWK:\n" << jwk.to_pretty_string() << std::endl << std::endl;
      if (store_keys) {
        fn_public = prefix + keyid + "-pkey.pem";
        fn_private = prefix + keyid + "-key.pem";
        jwk_file = prefix + keyid + "-sci.jwk";
        for (auto& pair : std::list<std::pair<std::string, std::string>>{
                 {fn_public, jwk.public_to_pem()},
                 {fn_private, jwk.private_to_pem()},
                 {jwk_file, jwk.to_pretty_string()}}) {
          std::ofstream file(pair.first);
          if (!file.is_open()) {
            std::cerr << "error: failed to open public key file " << pair.first
                      << std::endl;
            global_retc = EINVAL;
            return 0;
          }
          file << pair.second << std::endl;
          file.close();
        }
      }
      if (!fn_public.empty() && !fn_private.empty()) {
        std::cerr << (store_keys ? "Wrote" : "Used")
                  << " public key :  " << fn_public << std::endl
                  << (store_keys ? "Wrote" : "Used")
                  << " private key: " << fn_private << std::endl;
        if (!jwk_file.empty()) {
          std::cerr << "Wrote JWK file   : " << jwk_file << std::endl;
        }
      }
      return 0;
    }
    printHelp();
    global_retc = EINVAL;
    return 0;
#endif
  }
  void
  printHelp() const override
  {
    std::ostringstream oss;
    oss << "Usage: scitoken create|dump|create-keys\n"
        << "    command for handling scitokens generated by EOS\n"
        << std::endl
        << "  scitoken create --issuer <issuer> --keyid <keyid> [--profile <profile>] "
        << "--claim <claim-1> {... --claim <claim-n>} [--privkey <private-key-file>] "
        << "[--pubkey <public-key-file>] [--expires unix-ts]\n"
        << "    create a scitoken for a given keyid, issuer, profile containing claims\n"
        << "    <issuer>           : URL of the issuer\n"
        << "    <keyid>            : key id to request from the issuer\n"
        << "    <profile>          : token profile, one of \"wlcg\" [default], \"scitokens1\", "
        << "\"scitokens2\", \"atjwt\"\n"
        << "    <claims>           : <key>=<value> e.g. scope=storage.read:/eos/, scope=storage.modify:/eos/ ...\n"
        << "    <private-key-file> : file with the private key in PEM format - default /eos/xrootd/<keyid>-key.pem\n"
        << "    <public-key-file>  : file with the public key in PEM format - default /eos/xrootd/<keyid>-pkey.pem\n"
        << std::endl
        << "  scitoken dump <token>\n"
        << "    base64 decode a scitokens without verification\n"
        << std::endl
        << "  scitoken create-keys [--keyid <keyid>]\n"
        << "    create a PEM key pair and a JSON public web key. If <keyid> is specified \n"
        << "    then the pub/priv key pair is in /eos/xrootd/<keyid>-{key,pkey}.pem.\n"
        << "    Otherwise they are stored in CWD in default-{key,pkey}.pem. The JSON web \n"
        << "    key is printed on stdout, and the key locations on stderr.\n"
        << std::endl
        << "  Examples:\n"
        << "    eos scitoken create --issuer eos.cern.ch --keyid eos profile wlcg --claim sub=foo --claim scope=storage.read:/eos\n"
        << "    eos scitoken dump eyJhb ...\n"
        << "    eos scitoken create-keys --keyid eos > /etc/xrootd/eos.jwk\n";
    fprintf(stderr, "%s", oss.str().c_str());
  }
};
} // namespace

void
RegisterScitokenNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ScitokenCommand>());
}
