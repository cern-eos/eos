//------------------------------------------------------------------------------
// Start the MGM HTTP/2 listener. The xrootd protocol keeps its own port.
//------------------------------------------------------------------------------
#ifndef EOS_MGM_XIO_SERVER_HH
#define EOS_MGM_XIO_SERVER_HH

namespace eos {
namespace mgm {

// Binds when EOS_MGM_XIO_PORT is set. Returns 0 when disabled or listening.
int StartXioServer();
void StopXioServer();

} // namespace mgm
} // namespace eos

#endif
