//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Test Helpers
//------------------------------------------------------------------------------

#ifndef EOS_NS_TEST_HELPERS
#define EOS_NS_TEST_HELPERS

#include <string>

//------------------------------------------------------------------------------
//! Create an unique temporary file name
//------------------------------------------------------------------------------
std::string getTempName( std::string dir, std::string prefix );

#endif // EOS_NS_TEST_HELPERS
