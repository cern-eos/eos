#ifdef HIREDIS_FOUND

#include "ConfigEngineTest.hh"


using namespace std;


void ConfigEngineTest::setUp()
{
  engine = new eos::mgm::RedisConfigEngine("/var/eos/config/eos-dev01.cern.ch/",
      "localhost", 6379);
}

void ConfigEngineTest::tearDown()
{
  delete engine;
}

void ConfigEngineTest::ListConfigsTest()
{
  XrdOucString list = "";
  CPPUNIT_ASSERT(engine->ListConfigs(list, true));
  cout << "Config List output:\n" << list.c_str() << endl;
  CPPUNIT_ASSERT(list.find("default") != -1);
}

void ConfigEngineTest::LoadConfigTest()
{
  XrdOucEnv env =
    XrdOucEnv("mgm.cmd=config&mgm.subcmd=load&mgm.config.file=default");
  XrdOucString err;
  CPPUNIT_ASSERT(engine->LoadConfig(env, err));
  cout << "Err output:\n" << err.c_str() << endl;
  CPPUNIT_ASSERT(err == "");
}

int main(int argc, char** argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry& registry =
    CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest(registry.makeTest());
  runner.run();
  return 0;
}

#endif
