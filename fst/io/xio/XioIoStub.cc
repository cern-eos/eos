//------------------------------------------------------------------------------
// Placeholder when the XioFS library is not linked. xiohttp:// then fails
// closed instead of being mistaken for a local path.
//------------------------------------------------------------------------------
#include "fst/io/FileIo.hh"
#include "common/Logging.hh"

EOSFSTNAMESPACE_BEGIN

FileIo* MakeXioIo(const std::string&)
{
  eos_static_warning("xiohttp requested but this build has no XioFS library");
  return nullptr;
}

EOSFSTNAMESPACE_END
