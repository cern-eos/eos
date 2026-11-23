//------------------------------------------------------------------------------
// HTTP/2 opaque query and stat against an Xio listener.
//------------------------------------------------------------------------------
#ifndef EOS_COMMON_XIO_QUERY_HH
#define EOS_COMMON_XIO_QUERY_HH

#include <functional>
#include <string>
#include <sys/stat.h>

namespace eos {
namespace common {

// POST opaque CGI to http://host:port/. Sends Authorization: Bearer when
// EOS_XIO_INTERNAL_TOKEN is set. Returns 0 and the response body on HTTP 200.
// When http_status is set, it receives the HTTP status if a response arrived,
// or 0 when the listener could not be reached.
// path is the URL path. The opaque CGI is appended after '?'.
int XioOpaqueQuery(const std::string& host, int port, const std::string& opaque,
                   std::string& body, std::string& err, int timeout_ms = 10000,
                   int* http_status = nullptr, const std::string& path = "/");

// Host portion of "host:port" or "[ipv6]:port".
std::string XioHostOf(const std::string& hostport);

// HEAD/PROPFIND getattr of path on that listener.
// HEAD of path. http_status is 0 when the listener is not reached and the
// HTTP status when a response arrived. A non-zero http_status with a non-zero
// return is the server's answer and must not be retried over root://.
int XioStat(const std::string& host, int port, const std::string& path,
            struct stat* st, std::string& err, int timeout_ms = 10000,
            int* http_status = nullptr);

// GET with x-xio-chksum. body is the checksum text the XRootD query returns,
// including the leading "eos " the callers strip. http_status is 0 when the
// listener is not reached.
int XioChecksum(const std::string& host, int port, const std::string& path,
                std::string& body, std::string& err, int timeout_ms = 10000,
                int* http_status = nullptr);

// Third-party copy. src_url and dst_url are http://host:port/path?cgi without
// tpc.* parameters. The destination FST pulls the bytes. cancel is polled
// while the copy runs. Returns 0, or a positive errno. ECANCELED when cancel
// returns true.
int XioTpcCopy(const std::string& src_url, const std::string& dst_url,
               int timeout_sec, std::string& err,
               const std::function<bool()>& cancel = {});

// Stream a local file to dst_url (http://host:port/path?cgi) in 1 MiB writes.
// The whole file is not held in memory. http_status is 0 when the listener is
// not reached. A non-zero http_status is the server's answer.
int XioStreamPut(const std::string& local_path, const std::string& dst_url,
                 std::string& err, int* http_status = nullptr);

} // namespace common
} // namespace eos

#endif
