// ----------------------------------------------------------------------
// File: com_scitoken.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"
#include "common/Utils.hh"
#include "common/Mapping.hh"
#include <scitokens/scitokens.h>
#include <getopt.h>
#include <stdlib.h>
#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

/* SciToken factory */
int
com_scitoken(char* arg1)
{
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetTokenUnquoted();
  std::string option;
  std::string value;

  std::string cred;
  std::string key;
  std::string creddata;
  std::string keydata;
  std::string keyid;
  std::string issuer;
  std::string profile="wlcg";
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

    eos::common::StringConversion::LoadFileIntoString(key.c_str(),keydata);
    if (keydata.empty()) {
      std::cerr << "error: cannot load private key from '" << key.c_str() << "'" << std::endl;
      global_retc = EINVAL;
      return (0);
    }

    eos::common::StringConversion::LoadFileIntoString(cred.c_str(),creddata);
    if (creddata.empty()) {
      std::cerr << "error: cannot load public key from '" << cred.c_str() << "'" << std::endl;
      global_retc = EINVAL;
      return (0);
    }
    
    // sci-token code
    char* err_msg=0;
    
    auto key_raw =
      scitoken_key_create(keyid.c_str(), "ES256", creddata.c_str(), keydata.c_str(), &err_msg);

    std::unique_ptr<void, decltype(&scitoken_key_destroy)> key(
        key_raw, scitoken_key_destroy);

    if (key_raw == nullptr) {
      std::cerr << "error: failed to generate a key: "<< err_msg<< std::endl;
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

    int rv = scitoken_set_claim_string(token.get(), "iss", issuer.c_str(),&err_msg);
    if (rv) {
      std::cerr << "error: failed to set issuer: " << err_msg << std::endl;
      free(err_msg);
      global_retc = EFAULT;
      return (0);
    }

    for (const auto &claim : claims) {
      auto pos = claim.find("=");
      if (pos == std::string::npos) {
	std::cerr << "error: claim must contain a '=' character: " << claim.c_str() << std::endl;
	global_retc = EFAULT;
	return (0);
      }
      
      auto key = claim.substr(0, pos);
      auto val = claim.substr(pos + 1);
      
      rv = scitoken_set_claim_string(token.get(), key.c_str(), val.c_str(),
				     &err_msg);
      if (rv) {
	std::cerr << "error: failed to set claim '" << key << "'='" << val << "' error:" << err_msg << std::endl;
	free(err_msg);
	global_retc = EFAULT;
	return (0);
      }
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
    char *value=0;
    rv = scitoken_serialize(token.get(), &value, &err_msg);
    if (rv) {
      std::cerr << "error: failed to serialize the token: " << err_msg << std::endl;
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
      std::cerr << "# ------------------------------------------------------------------------------- #" << std::endl;
      std::cerr << eos::common::Mapping::PrintJWT(stoken, false) << std::endl;
      std::cerr << "# ------------------------------------------------------------------------------- #" << std::endl;
      global_retc = 0;
    } catch (...) {
      std::cerr << "error: failed to print token" << std::endl;
      global_retc = EINVAL;
    }
    return (0);
  }

  if (subcommand == "create-keys") {
    std::cerr << "error: not implemented" << std::endl;
    global_retc = EOPNOTSUPP;
    return (0);
    
  }
  
com_scitoken_usage:
  std::cerr << "usage: scitoken create|dump|create-keys ..." << std::endl;
  std::cerr << "       scitoken create                       : create a sci token for agiven keyid,issuer, profiel containing claims [default profile=wlcg]" << std::endl;
  std::cerr << std::endl;
  std::cerr << "       scitoken create --issuer <issuer> --keyid <keyid> [--profile=<profile>] --claim <claim-1> {... --claim <claim-N>} [--privkey <private-key-file>] [--pubkey <public-key-file>] " << std::endl;
  std::cerr << "                                    <issuer> : URL of the issuer" << std::endl;
  std::cerr << "                                     <keyid> : key id to request from the issuer" << std::endl;
  std::cerr << "                                   <profile> : token profile - can be 'wlcg' 'scitokens1' 'scitokens2' 'atjwt'" << std::endl;
  std::cerr << "                                    <claims> : <key>=<value> e.g. scope=storage.read:/eos/, scope=storage.modify:/eos/ ..." << std::endl;
  std::cerr << "                          <private-key-file> : file with the private key in PEM format - default is /etc/xrootd/<keyid>-key.pem" << std::endl;
  std::cerr << "                           <public-key-file> : file with the public key in PEM format - default is /etc/xrootd/<keyid>-pkey.pem" << std::endl;
  std::cerr << std::endl;
  std::cerr << "       scitoken dump                         : base64 decode a scitoken - this does not verify the token" << std::endl;
  std::cerr << std::endl;
  std::cerr << "       scitoken dump <token>" << std::endl;
  std::cerr << std::endl;
  std::cerr << "       scitoken create-keys                  : create a pem key pair and a JSON public key file - NOT IMPLEMENTED!" << std::endl;

  std::cerr << "       Examples:                               eos scitoken create --issuer=eos.cern.ch --keyid=eos --profile=wlcg --claim scope:storage.read=/eos" << std::endl;
  std::cerr << "                                               eos scitoken dump eyJhb..." << std::endl;
  global_retc = EINVAL;
  return (0);
}
