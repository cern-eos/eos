//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   TestHelpers
//------------------------------------------------------------------------------

#include "namespace/tests/TestHelpers.hh"
#include <cstdio>
#include <cstdlib>

//------------------------------------------------------------------------------
// Create a temporary file name
//------------------------------------------------------------------------------
std::string getTempName( std::string dir, std::string prefix )
{
  char *name = tempnam( dir.c_str(), prefix.c_str() );
  std::string strName = name;
  free( name );
  return strName;
}
