case $- in
  *i*) ;;
  *)
    return 0 2>/dev/null || exit 0
    ;;
esac

if [[ -n "${BASH_VERSION:-}" ]] && shopt -q progcomp; then
  if [[ -r /etc/bash_completion.d/eos ]]; then
    . /etc/bash_completion.d/eos
  fi
fi
