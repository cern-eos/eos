#!/usr/bin/env sh

# Decide which xrootd binary to use
XROOTD_BINARY=/usr/bin/xrootd

if [ -x /opt/eos/xrootd/bin/xrootd ]; then
  XROOTD_BINARY=/opt/eos/xrootd/bin/xrootd
fi

# When EOS_ZSTD_LOGGING is enabled (via /etc/sysconfig/eos_env, imported by the
# systemd unit as EnvironmentFile=), the EOS Logging class ingests xrootd
# diagnostics from STDERR and writes them as ZSTD-compressed rotating segments.
# In that case we must NOT pass '-l <logfile>' to xrootd, or it would redirect
# stdout/stderr to that file and the Logging pipeline would see nothing.
case "$(echo "${EOS_ZSTD_LOGGING:-}" | tr '[:upper:]' '[:lower:]')" in
  1|true|yes|on) USE_STDERR=1 ;;
  *)            USE_STDERR=0 ;;
esac

ARGS='"$@"'
LOGGING_MODE=""

if [ "${USE_STDERR}" = "1" ]; then
  # Strip any '-l <path>' pair from the argument list so xrootd logs to stderr.
  ARGS=""
  LOGGING_MODE=" (EOS_ZSTD_LOGGING=${EOS_ZSTD_LOGGING} : logging to stderr)"
  SKIP=0
  for a in "$@"; do
    if [ "${SKIP}" = "1" ]; then
      SKIP=0
      continue
    fi
    case "${a}" in
      -l)
        SKIP=1
        ;;
      -l*)
        # '-l/path' form (no space) - drop it as well
        ;;
      *)
        ARGS="${ARGS} \"${a}\""
        ;;
    esac
  done
fi

echo "Using xrootd binary: ${XROOTD_BINARY}${LOGGING_MODE}"
eval "exec \"\${XROOTD_BINARY}\" ${ARGS}"
