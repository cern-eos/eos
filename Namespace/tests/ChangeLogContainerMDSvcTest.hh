//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog  container metadata service test
//------------------------------------------------------------------------------

#ifndef CHANGE_LOG_CONTAINER_MD_SVC_TEST_HH
#define CHANGE_LOG_CONTAINER_MD_SVC_TEST_HH

#include <cppunit/TestCase.h>

//------------------------------------------------------------------------------
// Generic file md tests
//------------------------------------------------------------------------------
class ChangeLogContainerMDSvcTest: public CppUnit::TestCase
{
  public:
    ChangeLogContainerMDSvcTest(): CppUnit::TestCase() {}
    virtual ~ChangeLogContainerMDSvcTest() {}
    void reloadTest();
    static CppUnit::Test *suite();
};

#endif // CHANGE_LOG_CONTAINER_MD_SVC_TEST_HH
