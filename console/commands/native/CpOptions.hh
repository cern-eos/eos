#pragma once

#include <string>
#include <vector>

/** Parsed options for cp command */
struct CpOptions {
  std::string rate;
  std::string streams = "0";
  std::string atomic;
  std::vector<std::string> sources;
  std::string target;
  bool recursive = false;
  bool summary = false;
  bool noprogress = false;
  bool append = false;
  bool makeparent = false;
  bool debug = false;
  int debug_level = 0;
  bool checksums = false;
  bool silent = false;
  bool nooverwrite = false;
  bool preserve = false;
  unsigned long depth = 0;
};

// Parse without performing any copy or changing the namespace.
bool ParseCpArgs(const std::vector<std::string>& args, CpOptions& opts);
