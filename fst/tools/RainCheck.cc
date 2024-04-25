/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include <XrdCl/XrdClFile.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucString.hh>
#include "common/StringSplit.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/layout/HeaderCRC.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
#include <string>

#define DEFAULTBUFFERSIZE (4 * 1024 * 1024)

std::pair<XrdCl::URL, std::string>
parseLocation(std::string& location)
{
  size_t spos = location.rfind("//");
  std::string address = location.substr(0, spos + 1);
  XrdCl::URL url(address);

  if (!url.IsValid()) {
    fprintf(stderr, "URL is invalid: %s", address.c_str());
    exit(-1);
  }

  std::string path = location.substr(spos + 1, std::string::npos);
  return std::make_pair(url, path);
}

std::string
openOpaque(XrdCl::URL& url, std::string& filePath)
{
  XrdCl::FileSystem fs(url);
  std::string request = filePath;

  if (request.find('?') == std::string::npos) {
    request += "?mgm.pcmd=open";
  } else {
    request += "&mgm.pcmd=open";
  }

  XrdCl::Buffer arg;
  arg.FromString(request);
  XrdCl::Buffer* response = nullptr;
  XrdCl::XRootDStatus status =
    fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (!status.IsOK()) {
    fprintf(stderr, "Could not open file %s: %s", filePath.c_str(),
            status.GetErrorMessage().c_str());
    exit(-1);
  }

  std::string res(response->GetBuffer(), response->GetSize());
  delete response;
  return res;
}

std::string
getCheckSum(XrdCl::URL& url, std::string& filePath)
{
  XrdCl::FileSystem fs(url);
  XrdCl::Buffer arg;
  arg.FromString(filePath);
  XrdCl::Buffer* response = nullptr;
  XrdCl::XRootDStatus status =
    fs.Query(XrdCl::QueryCode::Checksum, arg, response);

  if (!status.IsOK()) {
    fprintf(stderr, "Could not open file %s: %s", filePath.c_str(),
            status.GetErrorMessage().c_str());
    exit(-1);
  }

  std::string checkSumResponse = response->GetBuffer();
  delete response;
  auto checksum = eos::common::StringSplit(checkSumResponse, " ");

  if (checksum.size() != 2) {
    fprintf(stderr, "Could not get checksum of file\n");
    exit(-1);
  }

  return std::string(checksum[1]);
}

bool
isValidStripeCombination(const std::vector<std::string>& stripes,
                         const std::string& XS, eos::fst::CheckSum* xsObj,
                         eos::common::LayoutId::layoutid_t layout,
                         const std::string& opaqueInfo, char* buffer)
{
  eos::fst::RainMetaLayout* redundancyObj = nullptr;

  if (eos::common::LayoutId::GetLayoutType(layout) ==
      eos::common::LayoutId::kRaidDP) {
    redundancyObj = new eos::fst::RaidDpLayout(
      nullptr, layout, nullptr, nullptr, stripes.front().c_str(), 0, false);
  } else {
    redundancyObj = new eos::fst::ReedSLayout(
      nullptr, layout, nullptr, nullptr, stripes.front().c_str(), 0, false);
  }

  if (redundancyObj->OpenPio(stripes, 0, 0, opaqueInfo.c_str())) {
    redundancyObj->Close();
    delete redundancyObj;
    return false;
  }

  off_t offsetXrd = 0;
  xsObj->Reset();

  while (true) {
    int64_t nread = redundancyObj->Read(offsetXrd, buffer, DEFAULTBUFFERSIZE);

    if (nread == 0) {
      break;
    }

    if (nread == -1) {
      fprintf(stderr, "error: could not read from local stripes\n");
      redundancyObj->Close();
      delete redundancyObj;
      return false;
    }

    xsObj->Add(buffer, nread, offsetXrd);
    offsetXrd += nread;
  }

  redundancyObj->Close();
  delete redundancyObj;
  xsObj->Finalize();
  return !strcmp(xsObj->GetHexChecksum(), XS.c_str());
}

void
cleanup(int code, const std::vector<std::string>& stripePaths)
{
  for (const auto& path : stripePaths) {
    if (std::remove(path.c_str()) != 0) {
      fprintf(stderr, "Could not cleanup file: %s\n", path.c_str());
    }
  }

  exit(code);
}

int
getStripeId(const std::string& path)
{
  auto file{eos::fst::FileIoPlugin::GetIoObject(path)};

  if (file->fileOpen(0, 0)) {
    fprintf(stderr, "Could not open file %s\n", path.c_str());
    return -1;
  }

  auto* hd = new eos::fst::HeaderCRC(0, 0);
  hd->ReadFromFile(file, 0);
  int const id = hd->GetIdStripe();
  delete hd;
  return id;
}


//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------
int
main(int argc, char* argv[])
{
  if (argc != 2) {
    return -1;
  }

  std::string location = argv[1];
  auto [url, filePath] = parseLocation(location);
  std::string opaqueResponse = openOpaque(url, filePath);
  auto* opaqueEnv = new XrdOucEnv(opaqueResponse.c_str());
  std::string opaqueInfo = strstr(opaqueResponse.c_str(), "&mgm.logid");
  eos::common::LayoutId::layoutid_t layout = opaqueEnv->GetInt("mgm.lid");

  if (!eos::common::LayoutId::IsRain(layout)) {
    fprintf(stderr, "Layout is not rain\n");
    exit(-1);
  }

  int const nStripes = (int)eos::common::LayoutId::GetStripeNumber(layout) + 1;
  int const nParityStripes =
    (int)eos::common::LayoutId::GetRedundancyStripeNumber(layout);
  int const nDataStripes = nStripes - nParityStripes;
  fprintf(stdout, "Found file with %d stripes (%d data, %d parity)\n", nStripes,
          nDataStripes, nParityStripes);
  int qpos = filePath.rfind('?');

  if (qpos != STR_NPOS) {
    opaqueInfo += "&";
    opaqueInfo += filePath.substr(qpos + 1);
    filePath.erase(qpos);
  }

  std::string pio;
  std::string tag;
  std::vector<std::string> stripeUrls;
  stripeUrls.reserve(nStripes);

  for (int i = 0; i < nStripes; i++) {
    tag = SSTR("pio." << i);

    if (!opaqueEnv->Get(tag.c_str())) {
      fprintf(stderr, "msg=\"empty pio url in mgm response\"");
      exit(-1);
    }

    pio = opaqueEnv->Get(tag.c_str());
    stripeUrls.emplace_back(SSTR("root://" << pio << "/" << filePath));
  }

  delete opaqueEnv;
  std::string XS = getCheckSum(url, filePath);
  eos::fst::RainMetaLayout* redundancyObj = new eos::fst::ReedSLayout(
    nullptr, layout, nullptr, nullptr, stripeUrls.front().c_str(), 0, false);

  if (redundancyObj->OpenPio(stripeUrls, 0, 0, opaqueInfo.c_str())) {
    fprintf(stderr, "error: can not open RAID object for read/write\n");
    exit(-EIO);
  }

  char* buffer = new char[DEFAULTBUFFERSIZE];
  int pos = filePath.rfind('/');
  std::string fileName = filePath.substr(pos + 1);
  std::vector<std::string> stripePaths;
  stripePaths.reserve(nStripes);

  for (int i = 0; i < nStripes; ++i) {
    std::string dstPath =
      SSTR("/var/tmp/eos-rain-check." << fileName << '.' << std::to_string(i));
    int dst = open(dstPath.c_str(), O_RDWR | O_TRUNC | O_CREAT | O_CLOEXEC,
                   S_IRUSR | S_IWUSR);

    if (dst == -1) {
      fprintf(stderr, "Could not create destination file: %s\n",
              dstPath.c_str());
      cleanup(-1, stripePaths);
    }

    stripePaths.emplace_back(dstPath);
    off_t offsetXrd = 0;

    while (true) {
      int64_t nread =
        redundancyObj->ReadStripe(offsetXrd, buffer, DEFAULTBUFFERSIZE, i);
      offsetXrd += nread;

      if (nread == 0) {
        close(dst);
        break;
      }

      if (nread == -1) {
        fprintf(stderr, "stripe %d located %s has invalid data\n",
                getStripeId(stripePaths[i]), stripeUrls[i].c_str());
        close(dst);
        break;
      }

      if (write(dst, buffer, nread) != nread) {
        fprintf(stderr, "Could not write to file: %s\n", dstPath.c_str());
        cleanup(-1, stripePaths);
      }
    }
  }

  redundancyObj->Close();
  delete redundancyObj;
  std::vector<bool> combinations(nStripes, false);
  std::fill(combinations.begin(), combinations.begin() + nDataStripes, true);
  std::vector<std::string> stripeCombination(nStripes, std::string());
  std::set<int> validStripes;
  std::set<int> unknownStripes;
  std::set<int> invalidStripes;
  auto* xsObj = eos::fst::ChecksumPlugins::GetXsObj(
                  eos::common::LayoutId::GetChecksum(layout));

  if (!xsObj) {
    fprintf(stderr, "invalid xs_type\n");
    cleanup(-1, stripePaths);
  }

  // Try to find a valid stripe combination
  do {
    for (int i = 0; i < nStripes; i++) {
      if (combinations[i]) {
        stripeCombination[i] = stripePaths[i];
      } else {
        stripeCombination[i] = "";
      }
    }

    if (isValidStripeCombination(stripeCombination, XS, xsObj, layout,
                                 opaqueInfo, buffer)) {
      bool markInvalid = true;

      for (int i = 0; i < nStripes; i++) {
        if (combinations[i]) {
          markInvalid = false;
          validStripes.insert(i);
        } else {
          if (markInvalid) {
            // All combinations involving this stripe have already been checked
            invalidStripes.insert(i);
          } else {
            unknownStripes.insert(i);
          }
        }
      }

      break;
    }
  } while (std::prev_permutation(combinations.begin(), combinations.end()));

  if (validStripes.empty()) {
    fprintf(stderr,
            "could not find enough valid stripes to reconstruct the file");
    cleanup(-1, stripePaths);
  }

  // Found a valid combination, check the rest of the stripes
  for (auto stripeId : unknownStripes) {
    combinations.assign(nStripes, false);
    // Try combinations with 1 unknown stripe and `nDataStripes - 1` valid
    // stripes
    combinations[stripeId] = true;
    auto vsid = validStripes.begin();

    for (int i = 0; i < nDataStripes - 1; i++, vsid++) {
      combinations[*vsid] = true;
    }

    for (int i = 0; i < nStripes; i++) {
      // Paths need to keep the same idx in `stripesPath` and `pathsCombination`
      if (combinations[i]) {
        stripeCombination[i] = stripePaths[i];
      } else {
        stripeCombination[i] = "";
      }
    }

    if (isValidStripeCombination(stripeCombination, XS, xsObj, layout,
                                 opaqueInfo, buffer)) {
      validStripes.insert(stripeId);
    } else {
      invalidStripes.insert(stripeId);
    }
  }

  for (auto i : invalidStripes) {
    fprintf(stderr, "stripe %d with path %s is invalid\n",
            getStripeId(stripePaths[i]), stripeUrls[i].c_str());
  }

  cleanup(0, stripePaths);
}
