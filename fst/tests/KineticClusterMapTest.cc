#include "catch.hpp"
#include "fst/io/KineticClusterMap.hh"
#include <exception>


SCENARIO("KineticClusterMap Public API.", "[ClusterMap]"){
    GIVEN("An invalid path"){
        std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
        setenv("KINETIC_DRIVE_LOCATION", "nonExistingFileName", 1);

        KineticClusterMap kcm;
        THEN("Map is empty"){
            REQUIRE(kcm.getSize() == 0);
        }
        setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1);
    }

    GIVEN("A path to a non json file"){
        std::string location(getenv("KINETIC_DRIVE_LOCATION") ? getenv("KINETIC_DRIVE_LOCATION") : "" );
        setenv("KINETIC_DRIVE_LOCATION", "kinetic-test", 1);

        KineticClusterMap kcm;
        THEN("Map is empty"){
            REQUIRE(kcm.getSize() == 0);
        }
        setenv("KINETIC_DRIVE_LOCATION", location.c_str(), 1);
    }

    GIVEN("A valid path."){
        KineticClusterMap kcm;
        std::shared_ptr<KineticClusterInterface> c;

        THEN("Map size equals number of entries."){
            REQUIRE(kcm.getSize() == 2);
        }
        THEN("An existing id to a running device returns a working cluster."){
            REQUIRE(kcm.getCluster("SN1", c) == 0);
            REQUIRE(c->ok());
        }
        THEN("An existing id with an unreachable IP returns a broken cluster"){
            REQUIRE(kcm.getCluster("SN2", c) == 0);
            REQUIRE(c->ok() == false);
        }
        THEN("A nonexisting drive id returns ENODEV."){
            REQUIRE(kcm.getCluster("nonExistingID", c) == ENODEV);
        }
    }
}
