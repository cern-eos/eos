//------------------------------------------------------------------------------
// Start the FST HTTP/2 listener. The xrootd protocol keeps its own port.
//------------------------------------------------------------------------------
#ifndef EOS_FST_XIO_SERVER_HH
#define EOS_FST_XIO_SERVER_HH

namespace eos {
namespace fst {

// Binds when EOS_FST_XIO_PORT is set. Returns 0 when disabled or listening.
int StartXioServer();
void StopXioServer();
// Bound HTTP/2 port, or 0 when the listener is not running.
int XioPort();

} // namespace fst
} // namespace eos

#endif
