#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include <glog/logging.h>

#define KINETIC_JSON "EOS_FST_KINETIC_JSON"
#define JSON_FILE    "localkinetic.json"



int main( int argc, char* const argv[] )
{
    // Set environment variable so that KineticIo can find the simulator. 
    std::string envstore(getenv(KINETIC_JSON) ? getenv(KINETIC_JSON) : "" );
    setenv(KINETIC_JSON, JSON_FILE, 1);

    // Init google logging so the test cases don't spam us. 
    google::InitGoogleLogging("");
    
    int result = Catch::Session().run( argc, argv );

    // Reset environment variable back to its initial value. 
    setenv(KINETIC_JSON, envstore.c_str(), 1); 
    return result;
}