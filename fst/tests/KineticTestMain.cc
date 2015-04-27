#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include <glog/logging.h>

/* Assumes a simulator is started on localhost and JSON_FILE points to
   the localkinetic.json file supplied in the source folder. Preset is correct 
   when project is built in a build folder in-source. */
#define JSON_FILE    "../../../fst/tests/localkinetic.json"

int main( int argc, char* const argv[] )
{
    // Set environment variable so that KineticIo can find the simulator. 
    std::string envstore(getenv("EOS_FST_KINETIC_JSON") ? getenv("EOS_FST_KINETIC_JSON") : "" );
    setenv("EOS_FST_KINETIC_JSON", JSON_FILE, 1);

    // Init google logging so the test cases don't spam us. 
    google::InitGoogleLogging("");
    
    int result = Catch::Session().run( argc, argv );

    // Reset environment variable back to its initial value. 
    setenv("EOS_FST_KINETIC_JSON", envstore.c_str(), 1); 
    return result;
}