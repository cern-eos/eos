#include "catch.hpp"
#include "fst/io/KineticIo.hh"

SCENARIO("KineticIo Public API", "[Io]"){
    
    kinetic::ConnectionOptions cops;
    cops.host = "localhost";
    cops.port = 8443;
    cops.use_ssl = true;
    cops.user_id = 1;
    cops.hmac_key = "asdfasdf";
    
    std::unique_ptr<kinetic::BlockingKineticConnection> bcon;
    kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
    REQUIRE(factory.NewBlockingConnection(cops, bcon, 30).ok());
    REQUIRE(bcon->InstantErase("NULL").ok());
    
    eos::fst::KineticIo kio(10);
    std::string path("kinetic:wwn1:filename");
    
    int  buf_size = 64;
    char write_buf[] = "rcPOa12L3nhN5Cgvsa6Jlr3gn58VhazjA6oSpKacLFYqZBEu0khRwbWtEjge3BUA";
    char read_buf[buf_size];
    char null_buf[buf_size];
    memset (null_buf, 0, buf_size);
    
    GIVEN ("Open is not called first."){            
        REQUIRE(bcon->InstantErase("NULL").ok());
        
        THEN("All public operations (except Statfs) fail with ENXIO error code"){
            REQUIRE(kio.Read(0,read_buf,buf_size) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.ReadAsync(0,read_buf,buf_size) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.Write(0,write_buf,buf_size) == SFS_ERROR);
            REQUIRE(errno == ENXIO);
            
            REQUIRE(kio.WriteAsync(0,write_buf,buf_size) == SFS_ERROR);
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
            
            THEN("Factory function for Attribute class returns 0"){
                std::unique_ptr<eos::fst::KineticIo::Attr> a;
                a.reset( eos::fst::KineticIo::Attr::OpenAttr(path.c_str()) );
                REQUIRE(!a);
            }
        }
        
        THEN("Open fails without create flag."){
            REQUIRE(kio.Open(path.c_str(), 0) == SFS_ERROR);
            REQUIRE(errno==ENOENT);
        }
        
        THEN("Factory function for Attribute class returns 0"){
            std::unique_ptr<eos::fst::KineticIo::Attr> a;
            a.reset( eos::fst::KineticIo::Attr::OpenAttr(path.c_str()) );
            REQUIRE(!a);
         }
    }
        
    GIVEN("Open succeeds with create flag."){         
        REQUIRE(kio.Open(path.c_str(), SFS_O_CREAT) == SFS_OK);
        
        THEN("Factory function for Attribute class succeeds"){
            std::unique_ptr<eos::fst::KineticIo::Attr> a;
            a.reset( eos::fst::KineticIo::Attr::OpenAttr(path.c_str()) );
            REQUIRE(a);

            AND_THEN("attributes can be set and read-in again."){
                REQUIRE(a->Set("name", write_buf, buf_size) == true);
                size_t size = buf_size; 
                memset(read_buf, 0, buf_size);
                REQUIRE(a->Get("name",read_buf,size) == true);
                REQUIRE(size == buf_size);
                REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
            }
        }

        THEN("Writing is possible from object start."){
            REQUIRE(kio.Write(0, write_buf, buf_size) == buf_size);

            AND_THEN("Written data can be read in again."){
                memset(read_buf,0,buf_size);
                REQUIRE(kio.Read(0, read_buf, buf_size) == buf_size);
                REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
            }
        }

        THEN("Writing is possible from an offset."){
            REQUIRE(kio.Write(1000000,write_buf,buf_size) == buf_size);

            AND_THEN("Written data can be read in again."){
                memset(read_buf,0,buf_size);
                REQUIRE(kio.Read(1000000, read_buf, buf_size) == buf_size);
                REQUIRE(memcmp(write_buf,read_buf,buf_size) == 0);
            }
        }

        THEN("Reading is possible from object start even if nothing was written."){
            REQUIRE(kio.Read(0,read_buf,buf_size) == buf_size);
            REQUIRE(memcmp(read_buf,null_buf,buf_size) == 0);
        }

        THEN("Reading is possible from an offset even if nothing was written."){
            REQUIRE(kio.Read(1000000,read_buf,buf_size) == buf_size);
            REQUIRE(memcmp(read_buf,null_buf,buf_size) == 0);
        }

        THEN("Stat should succeed and report a file size of 0"){
            struct stat stbuf;
            REQUIRE(kio.Stat(&stbuf) == SFS_OK);
            REQUIRE(stbuf.st_blocks == 1);
            REQUIRE(stbuf.st_blksize == 1024*1024);
            REQUIRE(stbuf.st_size == 0);
        }

        AND_WHEN("The file is closed again. "){
            REQUIRE(kio.Close() == SFS_OK);

            THEN("Trying to open again fails with EEXIST if create flag is set."){
                REQUIRE(kio.Open(path.c_str(), SFS_O_CREAT) == SFS_ERROR);
                REQUIRE(errno == EEXIST);
            }

            THEN("The file can be opened without create flag."){
                REQUIRE(kio.Open(path.c_str(), 0) == SFS_OK);
            }
        }

        AND_WHEN("Writing data accross multiple chunks."){
            REQUIRE(kio.Write(KineticChunk::capacity-32, write_buf, buf_size) == buf_size);

            THEN("IO object can be synced."){
                REQUIRE(kio.Sync() == SFS_OK);
            }

            THEN("Stat will return the filesize."){
                struct stat stbuf;
                REQUIRE(kio.Stat(&stbuf) == SFS_OK);
                REQUIRE(stbuf.st_blocks == 2);
                REQUIRE(stbuf.st_blksize == KineticChunk::capacity);
                REQUIRE(stbuf.st_size == stbuf.st_blksize-32+buf_size);
            }

            THEN("The file can can be removed again."){
                REQUIRE(kio.Remove() == SFS_OK);
                REQUIRE(kio.Close() == SFS_OK);
                REQUIRE(kio.Open(path, SFS_O_CREAT) == SFS_OK);
            }
        }
    }
}
