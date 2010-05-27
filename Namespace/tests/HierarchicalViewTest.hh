//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   HierarchicalView test
//------------------------------------------------------------------------------

#ifndef HIERARCHICAL_VIEW_TEST_HH
#define HIERARCHICAL_VIEW_TEST_HH

#include <cppunit/TestCase.h>

//------------------------------------------------------------------------------
// Hierarchical View test
//------------------------------------------------------------------------------
class HierarchicalViewTest: public CppUnit::TestCase
{
  public:
    HierarchicalViewTest(): CppUnit::TestCase() {}
    virtual ~HierarchicalViewTest() {}
    void reloadTest();
    static CppUnit::Test *suite();
};

#endif // CHANGE_LOG_TEST_HH
