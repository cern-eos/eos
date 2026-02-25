// ----------------------------------------------------------------------
// File: LegacySymbols.cc
// Purpose: Provide legacy com_* symbol definitions (no registrations)
// ----------------------------------------------------------------------

// Include only legacy implementations still referenced by native wrappers
// Direct com_* dependencies from native commands
// com_cp/com_cat: provided by cp-cmd-native.cc
#include "console/commands/coms/com_squash.cc"
#include "console/commands/coms/com_rclone.cc"

