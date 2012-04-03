#include <string.h>
#include <stdio.h>

#include <microhttpd.h>
#include <sys/stat.h>
#include <unistd.h>
#define PAGE "<html><head><title>No such file or directory</title></head><body>No such file or directory</body></html>"

#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdPosix/XrdPosixXrootdPath.hh>
#include <XrdPosix/XrdPosixExtern.hh>


XrdPosixXrootd posixsingleton;
XrdPosixXrootPath XP;

struct dir_info {
  DIR* dir;
  char name[4096];
};

static ssize_t
file_reader (void *cls, uint64_t pos, char *buf, size_t max)
{
  unsigned long long file = (unsigned long long)cls;
  return XrdPosixXrootd::Pread(file,buf,max, pos);
}

static void
free_callback (void *cls)
{
  unsigned long long file = (unsigned long long)cls;
  XrdPosixXrootd::Close(file);
}

static void
dir_free_callback (void *cls)
{
  struct dir_info *dh = (struct dir_info*)cls;
  if ( (dh && dh->dir) )
    XrdPosixXrootd::Closedir(dh->dir);
  if (dh) 
    free(dh);
}

static ssize_t
dir_reader (void *cls, uint64_t pos, char *buf, size_t max)
{
  struct dir_info *dh = (struct dir_info*)cls;
  struct dirent *e;
  fprintf(stderr,"dir reader %d\n", max);
  if (!dh)
    return MHD_CONTENT_READER_END_OF_STREAM;

  do
    {
      fprintf(stderr,"Reading dir %d\n", dh->dir);
      e = XrdPosixXrootd::Readdir(dh->dir);
      fprintf(stderr,"e=%d\n", e);
      if (e == NULL)
        return MHD_CONTENT_READER_END_OF_STREAM;
  } while (e->d_name[0] == '.');

  char link[16384];
  snprintf(link,sizeof(link),"%s/%s", dh->name, e->d_name);
  int rsize = snprintf (buf, max,
                   "<a href=\"/%s\">%s</a><br>",
                   link,
                   e->d_name);
  fprintf(stderr,"%s\n", buf);
  return rsize;
}


static int
ahc_echo (void *cls,
          struct MHD_Connection *connection,
          const char *url,
          const char *method,
          const char *version,
          const char *upload_data,
	  size_t *upload_data_size, void **ptr)
{
  static int aptr;
  struct MHD_Response *response;
  int ret=0;
  int file;
  DIR* dir;

  struct stat buf;

  if (0 != strcmp (method, MHD_HTTP_METHOD_GET))
    return MHD_NO;              /* unexpected method */
  if (&aptr != *ptr)
    {
      /* do never respond on first call */
      *ptr = &aptr;
      return MHD_YES;
    }
  *ptr = NULL;                  /* reset when done */


  const char *myPath;
  char buff[2048];
  if (!(myPath = XP.URL(&url[1], buff, sizeof(buff))))
    myPath=&url[1];

  file = 0;
  dir  = 0; 
  fprintf(stderr,"Openning %s %d\n", myPath, XrdPosixXrootd::Stat(myPath, &buf));
  if( (XrdPosixXrootd::Stat(myPath, &buf) == 0 ) ) {
    if (S_ISREG (buf.st_mode)) {
      // this is a file to open
      file = XrdPosixXrootd::Open(myPath,0,0);
    }
    if (S_ISDIR (buf.st_mode)) {
      // this is a dir to open
      dir = XrdPosixXrootd::Opendir(myPath);
    }
  } 

  if ( (!file) && (!dir) ) 
    {
      response = MHD_create_response_from_buffer (strlen (PAGE),
						  (void *) PAGE,
						  MHD_RESPMEM_PERSISTENT);
      ret = MHD_queue_response (connection, MHD_HTTP_NOT_FOUND, response);
      MHD_destroy_response (response);
    }
  else
    {
      response = 0;
      if (file) {
	response = MHD_create_response_from_callback (buf.st_size, 32 * 1024,     /* 32k page size */
						      &file_reader,
						      (void*)file,
						      &free_callback);
	if (response == NULL)
	  {
	    XrdPosixXrootd::Close(file);
	    return MHD_NO;
	  }
	ret = MHD_queue_response (connection, MHD_HTTP_OK, response);
	MHD_destroy_response (response);
      }
      if (dir) {
	struct dir_info* dir_handle = (struct dir_info*) malloc (sizeof (struct dir_info));
	if (dir_handle) {snprintf(dir_handle->name,sizeof(dir_handle->name), "%s", myPath);}
	dir_handle->dir = dir;
	response = MHD_create_response_from_callback (-1, 32 * 1024,     /* 32k page size */
						      &dir_reader,
						      (void*)dir_handle,
						      &dir_free_callback);
	if (response == NULL)
	  {
	    XrdPosixXrootd::Closedir(dir);
	    if (dir_handle)
	      free (dir_handle);
	    return MHD_NO;
	  }
	ret = MHD_queue_response (connection, MHD_HTTP_OK, response);
	MHD_destroy_response (response);
      }
    }
  return ret;
}

int
main (int argc, char *const *argv)
{
  struct MHD_Daemon *d;
  time_t end;
  time_t t;
  struct timeval tv;
  fd_set rs;
  fd_set ws;
  fd_set es;
  int max;
  unsigned MHD_LONG_LONG mhd_timeout;

  XrdPosixXrootd::setEnv("ReadAheadSize",           1024*1024);
  XrdPosixXrootd::setEnv("ReadCacheSize",       512*1024*1024);
  
  if (argc != 3)
    {
      printf ("%s PORT SECONDS-TO-RUN\n", argv[0]);
      return 1;
    }
  d = MHD_start_daemon (MHD_USE_DEBUG,
                        atoi (argv[1]),
                        NULL, NULL, &ahc_echo, (void*)PAGE, MHD_OPTION_END);
  if (d == NULL)
    return 1;
  end = time (NULL) + atoi (argv[2]);
  while ((t = time (NULL)) < end)
    {
      tv.tv_sec = end - t;
      tv.tv_usec = 0;
      max = 0;
      FD_ZERO (&rs);
      FD_ZERO (&ws);
      FD_ZERO (&es);
      if (MHD_YES != MHD_get_fdset (d, &rs, &ws, &es, &max))
	break; /* fatal internal error */
      if (MHD_get_timeout (d, &mhd_timeout) == MHD_YES)

        {
          if ((tv.tv_sec * 1000) < ( long long)mhd_timeout)
            {
              tv.tv_sec = mhd_timeout / 1000;
              tv.tv_usec = (mhd_timeout - (tv.tv_sec * 1000)) * 1000;
            }
        }
      select (max + 1, &rs, &ws, &es, &tv);
      MHD_run (d);
    }
  MHD_stop_daemon (d);
  return 0;
}
