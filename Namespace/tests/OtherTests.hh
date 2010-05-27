//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Other tests
//------------------------------------------------------------------------------

#ifndef OTHER_TESTS_HH
#define OTHER_TESTS_HH

#include <cppunit/TestCase.h>

//------------------------------------------------------------------------------
// Other tests
//------------------------------------------------------------------------------
class OtherTests: public CppUnit::TestCase
{
  public:
    OtherTests(): CppUnit::TestCase() {}
    virtual ~OtherTests() {}
    void pathSplitterTest();
    static CppUnit::Test *suite();
};

#endif // OTHER_TESTS_HH
