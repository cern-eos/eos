//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog test
//------------------------------------------------------------------------------

#ifndef CHANGE_LOG_TEST_HH
#define CHANGE_LOG_TEST_HH

#include <cppunit/TestCase.h>

//------------------------------------------------------------------------------
// Generic file md tests
//------------------------------------------------------------------------------
class ChangeLogTest: public CppUnit::TestCase
{
  public:
    ChangeLogTest(): CppUnit::TestCase() {}
    virtual ~ChangeLogTest() {}
    void readWriteCorrectness();
    void followingTest();
    static CppUnit::Test *suite();
};

#endif // CHANGE_LOG_TEST_HH
