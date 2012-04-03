#include <string.h>
#include <stdio.h>

#include <microhttpd.h>
#include <sys/stat.h>
#include <unistd.h>

#define PAGE "<html><head><title>File not found</title></head><body>File not found</body></html>"

#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdPosix/XrdPosixXrootdPath.hh>
#include <XrdPosix/XrdPosixExtern.hh>

XrdPosixXrootd posixsingleton;
XrdPosixXrootPath XP;

static ssize_t
file_reader (void *cls, uint64_t pos, char *buf, size_t max)
{
  int file = (int) cls;
  return XrdPosixXrootd::Pread(file,buf,max, pos);
}

static void
free_callback (void *cls)
{
  int file = cls;
  XrdPosixXrootd::Close(file);
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
  int ret;
  int file;
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


  char *myPath, buff[2048];
  if (!(myPath = XP.URL(&url[1], buff, sizeof(buff))))
    myPath=&url[1];
  
  
  if( (XrdPosixXrootd::Stat(myPath, &buf) == 0) &&
      (S_ISREG (buf.st_mode)) ) {
    file = XrdPosixXrootd::Open(myPath);
  else
    file = 0;
  if (file == 0)
    {
      response = MHD_create_response_from_buffer (strlen (PAGE),
						  (void *) PAGE,
						  MHD_RESPMEM_PERSISTENT);
      ret = MHD_queue_response (connection, MHD_HTTP_NOT_FOUND, response);
      MHD_destroy_response (response);
    }
  else
    {
      response = MHD_create_response_from_callback (buf.st_size, 32 * 1024,     /* 32k page size */
                                                    &file_reader,
                                                    file,
                                                    &free_callback);
      if (response == NULL)
	{
	  XrdPosixXrootd::Close(file);
	  return MHD_NO;
	}
      ret = MHD_queue_response (connection, MHD_HTTP_OK, response);
      MHD_destroy_response (response);
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

  if (argc != 3)
    {
      printf ("%s PORT SECONDS-TO-RUN\n", argv[0]);
      return 1;
    }
  d = MHD_start_daemon (MHD_USE_DEBUG,
                        atoi (argv[1]),
                        NULL, NULL, &ahc_echo, PAGE, MHD_OPTION_END);
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
          if (tv.tv_sec * 1000 < mhd_timeout)
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
