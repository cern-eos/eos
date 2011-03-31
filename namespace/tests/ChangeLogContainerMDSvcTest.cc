//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog test
//------------------------------------------------------------------------------

#include <cppunit/extensions/HelperMacros.h>
#include <stdint.h>
#include <unistd.h>

#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/tests/TestHelpers.hh"

//------------------------------------------------------------------------------
// Declaration
//------------------------------------------------------------------------------
class ChangeLogContainerMDSvcTest: public CppUnit::TestCase
{
  public:
    CPPUNIT_TEST_SUITE( ChangeLogContainerMDSvcTest );
      CPPUNIT_TEST( reloadTest );
    CPPUNIT_TEST_SUITE_END();

    void reloadTest();
};

CPPUNIT_TEST_SUITE_REGISTRATION( ChangeLogContainerMDSvcTest );

//------------------------------------------------------------------------------
// Concrete implementation tests
//------------------------------------------------------------------------------
void ChangeLogContainerMDSvcTest::reloadTest()
{
  try
  {
    eos::IContainerMDSvc *containerSvc = new eos::ChangeLogContainerMDSvc;
    std::map<std::string, std::string> config;
    std::string fileName = getTempName( "/tmp", "eosns" );
    config["changelog_path"] = fileName;
    containerSvc->configure( config );
    containerSvc->initialize();
  
    eos::ContainerMD *container1 = containerSvc->createContainer();
    eos::ContainerMD *container2 = containerSvc->createContainer();
    eos::ContainerMD *container3 = containerSvc->createContainer(); 
    eos::ContainerMD *container4 = containerSvc->createContainer();
    eos::ContainerMD *container5 = containerSvc->createContainer();

    eos::ContainerMD::id_t id = container1->getId();

    container1->setName( "root" );
    container1->setParentId( container1->getId() );
    container2->setName( "subContLevel1-1" );
    container3->setName( "subContLevel1-2" );
    container4->setName( "subContLevel2-1" );
    container5->setName( "subContLevel2-2" );

    container5->setCUid( 17 );
    container5->setCGid( 17 );
    container5->setMode( 0750 );

    CPPUNIT_ASSERT( container5->access( 17, 12, X_OK|R_OK|W_OK ) == true);
    CPPUNIT_ASSERT( container5->access( 17, 12, X_OK|R_OK ) == true );
    CPPUNIT_ASSERT( container5->access( 12, 17, X_OK|R_OK|W_OK ) == false );
    CPPUNIT_ASSERT( container5->access( 12, 17, X_OK|W_OK ) == false );
    CPPUNIT_ASSERT( container5->access( 12, 17, X_OK|R_OK ) == true );
    CPPUNIT_ASSERT( container5->access( 12, 12, X_OK|R_OK ) == false );

    container1->addContainer( container2 );
    container1->addContainer( container3 );
    container3->addContainer( container4 );
    container3->addContainer( container5 );

    containerSvc->updateStore( container1 );
    containerSvc->updateStore( container2 );
    containerSvc->updateStore( container3 );
    containerSvc->updateStore( container4 );
    containerSvc->updateStore( container5 );

    container3->removeContainer( "subContainerLevel2-2" );
    containerSvc->removeContainer( container5 );

    eos::ContainerMD *container6 = containerSvc->createContainer();
    container6->setName( "subContLevel2-3" );
    container3->addContainer( container6 );
    containerSvc->updateStore( container6 );

    eos::ContainerMD::id_t idAttr = container4->getId();
    container4->setAttribute( "test1", "test1" );
    container4->setAttribute( "test1", "test11" );
    container4->setAttribute( "test2", "test2" );
    container4->setAttribute( "test3", "test3" );
    containerSvc->updateStore( container4 );

    CPPUNIT_ASSERT( container4->numAttributes() == 3 );
    CPPUNIT_ASSERT( container4->getAttribute( "test1" ) == "test11" );
    CPPUNIT_ASSERT( container4->getAttribute( "test3" ) == "test3" );
    CPPUNIT_ASSERT_THROW( container4->getAttribute( "test15" ), eos::MDException );

    containerSvc->finalize();

    containerSvc->initialize();
    eos::ContainerMD *cont1 = containerSvc->getContainerMD( id );
    CPPUNIT_ASSERT( cont1->getName() == "root" );

    eos::ContainerMD *cont2 = cont1->findContainer( "subContLevel1-1" );
    CPPUNIT_ASSERT( cont2 != 0 );
    CPPUNIT_ASSERT( cont2->getName() == "subContLevel1-1" );

    cont2 = cont1->findContainer( "subContLevel1-2" );
    CPPUNIT_ASSERT( cont2 != 0 );
    CPPUNIT_ASSERT( cont2->getName() == "subContLevel1-2" );

    cont1 = cont2->findContainer( "subContLevel2-1" );
    CPPUNIT_ASSERT( cont1 != 0 );
    CPPUNIT_ASSERT( cont1->getName() == "subContLevel2-1" );

    cont1 = cont2->findContainer( "subContLevel2-2" );
    CPPUNIT_ASSERT( cont1 == 0 );

    cont1 = cont2->findContainer( "subContLevel2-3" );
    CPPUNIT_ASSERT( cont1 != 0 );
    CPPUNIT_ASSERT( cont1->getName() == "subContLevel2-3" );

    eos::ContainerMD *contAttrs = containerSvc->getContainerMD( idAttr );
    CPPUNIT_ASSERT( contAttrs->numAttributes() == 3 );
    CPPUNIT_ASSERT( contAttrs->getAttribute( "test1" ) == "test11" );
    CPPUNIT_ASSERT( contAttrs->getAttribute( "test3" ) == "test3" );
    CPPUNIT_ASSERT_THROW( contAttrs->getAttribute( "test15" ), eos::MDException );

    containerSvc->finalize();


    delete containerSvc;
    unlink( fileName.c_str() );
  }
  catch( eos::MDException &e )
  {
    CPPUNIT_ASSERT_MESSAGE( e.getMessage().str(), false );
  }
}
