//------------------------------------------------------------------------------
// File: com_scitoken.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#ifdef __APPLE__
int
com_scitoken(char* arg1)
{
  fprintf(stderr, "error: scitoken command is not support on OSX\n");
  global_retc = EINVAL;
  return (0);
}

#else
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/jwk_generator/jwk_generator.hpp"
#include "common/Mapping.hh"
#include "common/StringTokenizer.hh"
#include "common/Utils.hh"
#include <common/Logging.hh>
#include <cstdio>
#include <fstream>
#include <getopt.h>
#include <json/json.h>
#include <memory>
#include <scitokens/scitokens.h>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

//------------------------------------------------------------------------------
// SciToken command
//------------------------------------------------------------------------------
int
com_scitoken(char* arg1)
{
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetTokenUnquoted();
  std::string option;
  std::string value;
  time_t expires = 0;
  std::string cred;
  std::string key;
  std::string creddata;
  std::string keydata;
  std::string keyid;
  std::string issuer;
  std::string jwk;
  std::string profile = "wlcg";
  std::set<std::string> claims;

  if (wants_help(arg1)) {
    goto com_scitoken_usage;
  }

  if (subcommand == "create") {
    do {
      const char* o = subtokenizer.GetTokenUnquoted();
      const char* v = subtokenizer.GetTokenUnquoted();

      if (o && !v) {
        goto com_scitoken_usage;
      }

      if (!o && !v) {
        break;
      }

      option = o;
      value = v;

      if (option == "--pubkey") {
        cred = value;
      }

      if (option == "--privkey") {
        key = value;
      }

      if (option == "--keyid") {
        keyid = value;
      }

      if (option == "--issuer") {
        issuer = value;
      }

      if (option == "--claim") {
        claims.insert(value);
      }

      if (option == "--expires") {
        expires = atoi(value.c_str());
      }

      if (option == "--profile") {
        profile = value;
      }
    } while (option.length());

    if (issuer.empty() || !claims.size() || keyid.empty()) {
      goto com_scitoken_usage;
    }

    if (cred.empty()) {
      cred = "/etc/xrootd/";
      cred += keyid;
      cred += "-pkey.pem";
    }

    if (key.empty()) {
      key = "/etc/xrootd/";
      key += keyid;
      key += "-key.pem";
    }

    eos::common::StringConversion::LoadFileIntoString(key.c_str(), keydata);

    if (keydata.empty()) {
      std::cerr << "error: cannot load private key from '" << key.c_str() << "'"
                << std::endl;
      global_retc = EINVAL;
      return (0);
    }

    eos::common::StringConversion::LoadFileIntoString(cred.c_str(), creddata);

    if (creddata.empty()) {
      std::cerr << "error: cannot load public key from '" << cred.c_str() << "'"
                << std::endl;
      global_retc = EINVAL;
      return (0);
    }

    // sci-token code
    char* err_msg = 0;
    auto key_raw = scitoken_key_create(keyid.c_str(), "ES256", creddata.c_str(),
                                       keydata.c_str(), &err_msg);
    std::unique_ptr<void, decltype(&scitoken_key_destroy)> key(
      key_raw, scitoken_key_destroy);

    if (key_raw == nullptr) {
      std::cerr << "error: failed to generate a key: " << err_msg << std::endl;
      free(err_msg);
      global_retc = EFAULT;
      return (0);
    }

    std::unique_ptr<void, decltype(&scitoken_destroy)> token(
      scitoken_create(key_raw), scitoken_destroy);

    if (token.get() == nullptr) {
      std::cerr << "error: failed to generate a new token" << std::endl;
      global_retc = EFAULT;
      return (0);
    }

    int rv =
      scitoken_set_claim_string(token.get(), "iss", issuer.c_str(), &err_msg);

    if (rv) {
      std::cerr << "error: failed to set issuer: " << err_msg << std::endl;
      free(err_msg);
      global_retc = EFAULT;
      return (0);
    }

    for (const auto& claim : claims) {
      auto pos = claim.find("=");

      if (pos == std::string::npos) {
        std::cerr << "error: claim must contain a '=' character: "
                  << claim.c_str() << std::endl;
        global_retc = EFAULT;
        return (0);
      }

      auto key = claim.substr(0, pos);
      auto val = claim.substr(pos + 1);
      rv = scitoken_set_claim_string(token.get(), key.c_str(), val.c_str(),
                                     &err_msg);

      if (rv) {
        std::cerr << "error: failed to set claim '" << key << "'='" << val
                  << "' error:" << err_msg << std::endl;
        free(err_msg);
        global_retc = EFAULT;
        return (0);
      }
    }

    if (expires) {
      auto lifetime = expires - time(NULL);

      if (lifetime < 0) {
        lifetime = 0;
      }

      scitoken_set_lifetime(token.get(), lifetime);
    }

    if (profile == "wlcg") {
      profile = SciTokenProfile::WLCG_1_0;
    } else if (profile == "scitokens1") {
      profile = SciTokenProfile::SCITOKENS_1_0;
    } else if (profile == "scitokens2") {
      profile = SciTokenProfile::SCITOKENS_2_0;
    } else if (profile == "atjwt") {
      profile = SciTokenProfile::AT_JWT;
    } else {
      std::cerr << "error: unknown token profile: " << profile << std::endl;
      global_retc = EINVAL;
      return (0);
    }

    SciTokenProfile sprofile = WLCG_1_0;
    scitoken_set_serialize_mode(token.get(), sprofile);
    // finalue dump the token
    char* value = 0;
    rv = scitoken_serialize(token.get(), &value, &err_msg);

    if (rv) {
      std::cerr << "error: failed to serialize the token: " << err_msg
                << std::endl;
      free(err_msg);
      global_retc = EFAULT;
      return (0);
    }

    std::cout << value << std::endl;
    return (0);
  }

  if (subcommand == "dump") {
    XrdOucString token = subtokenizer.GetTokenUnquoted();

    if (!token.length()) {
      goto com_scitoken_usage;
    }

    try {
      std::string stoken = token.c_str();
      std::cerr << "# "
                "-----------------------------------------------------------"
                "-------------------- #"
                << std::endl;
      std::cerr << eos::common::Mapping::PrintJWT(stoken, false) << std::endl;
      std::cerr << "# "
                "-----------------------------------------------------------"
                "-------------------- #"
                << std::endl;
      global_retc = 0;
    } catch (...) {
      std::cerr << "error: failed to print token" << std::endl;
      global_retc = EINVAL;
    }

    return (0);
  }

  if (subcommand == "create-keys") {
    do {
      const char* o = subtokenizer.GetTokenUnquoted();
      const char* v = subtokenizer.GetTokenUnquoted();

      if (o && !v) {
        goto com_scitoken_usage;
      }

      if (!o && !v) {
        break;
      }

      option = o;
      value = v;

      if (option == "--keyid") {
        keyid = value;
      }
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

    if (*prefix.rbegin() != '/') {
      prefix += '/';
    }

    // If the public/private key files exist then we use them to generate
    // the jwk file, otherwise we generate new keys
    bool store_keys = false;
    std::string fn_public = SSTR(prefix << keyid << "-pkey.pem").c_str();
    std::string fn_private = SSTR(prefix << keyid << "-key.pem").c_str();
    std::string jwk_file;
    struct stat buf;

    if (::stat(fn_public.c_str(), &buf) ||
        ::stat(fn_private.c_str(), &buf)) {
      // We generate new keys
      fn_public = "";
      fn_private = "";
      store_keys = true;
    }

    using namespace jwk_generator;
    JwkGenerator<ES256> jwk(keyid, fn_public, fn_private);
    std::cout << "JWK:\n" << jwk.to_pretty_string()
              << std::endl << std::endl;

    if (store_keys)  {
      fn_public = SSTR(prefix << keyid << "-pkey.pem").c_str();
      fn_private = SSTR(prefix << keyid << "-key.pem").c_str();
      jwk_file = SSTR(prefix << keyid << "-sci.jwk").c_str();

      for (auto pair : std::list<std::pair<std::string, std::string>> {
      {fn_public, jwk.public_to_pem()},
        {fn_private, jwk.private_to_pem()},
        {jwk_file, jwk.to_pretty_string()}
      }) {
        std::ofstream file(pair.first);

        if (!file.is_open()) {
          std::cerr << "error: failed to open public key file "
                    << pair.first << std::endl;
          global_retc = EINVAL;
          return 0;
        }

        file << pair.second << std::endl;
        file.close();
      }
    }

    if (!fn_public.empty() && !fn_private.empty()) {
      std::cerr << (store_keys ? "Wrote" : "Used") << " public key :  "
                << fn_public << std::endl
                << (store_keys ? "Wrote" : "Used") << " private key: "
                << fn_private << std::endl;

      if (!jwk_file.empty()) {
        std::cerr << "Wrote JWK file   : " << jwk_file << std::endl;
      }
    }

    return 0;
  }

com_scitoken_usage:
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
      << "    create a PEM key pair and a JSON public we key. If <keyid> is specified \n"
      << "    then the pub/priv key pair is in /eos/xrootd/<keyid>-{key,pkey}.pem.\n"
      << "    Otherwise they are stored in CWD in default-{key,pkey}.pem. The JSON web \n"
      << "    key is printed on stdout, and the key locations on stderr.\n"
      << std::endl
      << "  Examples:\n"
      << "    eos scitoken create --issuer eos.cern.ch --keyid eos "
      << "profile wlcg --claim sub=foo --claim scope=storage.read:/eos\n"
      << "    eos scitoken dump eyJhb ...\n"
      << "    eos scitoken create-keys --keyid eos > /etc/xrootd/eos.jwk\n";
  std::cerr << oss.str().c_str() << std::endl;
  global_retc = EINVAL;
  return 0;
}
#endif
