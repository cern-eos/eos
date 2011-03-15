//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Some constants concerning the change log data
//------------------------------------------------------------------------------

#ifndef EOS_NS_CHANGELOG_CONSTANTS_HH
#define EOS_NS_CHANGELOG_CONSTANTS_HH

#include <stdint.h>

namespace eos
{
  extern const uint8_t  UPDATE_RECORD_MAGIC;
  extern const uint8_t  DELETE_RECORD_MAGIC;
  extern const uint16_t FILE_LOG_MAGIC;
  extern const uint16_t CONTAINER_LOG_MAGIC;
}

#endif // EOS_NS_CHANGELOG_CONSTANTS_HH
