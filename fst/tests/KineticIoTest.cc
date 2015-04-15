#include "catch.hpp"
#include "fst/io/KineticIo.hh"

SCENARIO("KineticIo handles incorrect input.", "[Io][errInput]"){
    GIVEN("An empty KineticIo object"){
        eos::fst::KineticIo kio(10);
        THEN("All public operations (except Statfs) fail with ENXIO error code if file is not opened."){
            char buffer[64];
            
            REQUIRE(kio.Read(0,buffer,64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.ReadAsync(0,buffer,64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Write(0,buffer,64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.WriteAsync(0,buffer,64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Truncate(0) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Fallocate(64) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Remove() == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Sync() == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Close() == SFS_ERROR);
            REQUIRE(errno == ENXIO);
        }
        THEN("Encapsulated attr functions fail"){
            eos::fst::KineticIo::Attr a ("path", kio);
            char buffer[64]; size_t size;
            REQUIRE(a.Get("name",buffer,size) == false);
            REQUIRE(a.Set("name",buffer,size) == false);
        }
        WHEN("Using an illegally constructed path"){
            std::string path("path");
            THEN("Calling Open fails with errno ENODEV"){
                REQUIRE(kio.Open(path.c_str(), SFS_O_CREAT) == SFS_ERROR);
                REQUIRE(errno == ENODEV);
            }
            THEN("Calling Statfs returns ENODEV"){
                struct statfs sfs;
                REQUIRE(kio.Statfs(path.c_str(), &sfs) == ENODEV);
            }
        }
    }
}

SCENARIO("KineticIo public API test.", "[Io][pub]"){
    
    kinetic::ConnectionOptions cops;
    cops.host = "localhost";
    cops.port = 8443;
    cops.use_ssl = true;
    cops.user_id = 1;
    cops.hmac_key = "asdfasdf";
    
    std::unique_ptr<kinetic::BlockingKineticConnection> bcon;
    kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
    REQUIRE(factory.NewBlockingConnection(cops, bcon, 30).ok());

    GIVEN ("An empty KineticIo object, an empty kinetic drive and a legal path"){
        eos::fst::KineticIo kio(10);
        std::string path("kinetic:wwn1:filename");
        REQUIRE(bcon->InstantErase("NULL").ok());

        THEN("Open fails without create flag."){
            REQUIRE(kio.Open(path.c_str(), 0) == SFS_ERROR);
        }
        WHEN("Open succeeds"){
            REQUIRE(kio.Open(path.c_str(), SFS_O_CREAT) == SFS_OK);

            THEN("Writing is possible from object start."){
                    REQUIRE(kio.Write(0, "123",3) == 3);
            }
            THEN("Writing is possible from any offset."){
                    REQUIRE(kio.Write(1000000,"123",3) == 3);
            }
            THEN("Reading is possible from object start."){
                    char buf[10];
                    REQUIRE(kio.Read(0,buf,10) == 10);
            }
            THEN("Reading is possible from any offset."){
                    char buf[10];
                    REQUIRE(kio.Read(1000000,buf,10) == 10);
            }
            THEN("Stat should succeed and report a file size of 0"){
                struct stat stbuf;
                REQUIRE(kio.Stat(&stbuf) == SFS_OK);
                REQUIRE(stbuf.st_blocks == 1);
                REQUIRE(stbuf.st_blksize == 1024*1024);
                REQUIRE(stbuf.st_size == 0);
            }

            AND_WHEN("Flushing written data."){
                    int buf_size = 64;
                    char buf[] = "rcPOa12L3nhN5Cgvsa6Jlr3gn58VhazjA6oSpKacLFYqZBEu0khRwbWtEjge3BUA";
                    for(off_t o=0; o <= 1024*1024; o+=buf_size)
                            REQUIRE(kio.Write(o, buf, buf_size) == buf_size);
                    REQUIRE(kio.Sync() == SFS_OK);

                    THEN("Stat will return the filesize."){
                            struct stat stbuf;
                            REQUIRE(kio.Stat(&stbuf) == SFS_OK);
                            REQUIRE(stbuf.st_blocks == 2);
                            REQUIRE(stbuf.st_blksize == 1024*1024);
                            REQUIRE(stbuf.st_size == stbuf.st_blksize+buf_size);
                    }
                    THEN("Open fails with create flag."){
                            REQUIRE(kio.Close() == SFS_OK);
                            REQUIRE(kio.Open("path", SFS_O_CREAT) == SFS_ERROR);
                    }
                    THEN("The file can can be removed again."){
                            REQUIRE(kio.Remove() == SFS_OK);
                            REQUIRE(kio.Close() == SFS_OK);
                            REQUIRE(kio.Open(path, SFS_O_CREAT) == SFS_OK);
                    }
            }
        }
    }
}
