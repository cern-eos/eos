# bats file_tags=traffic_shaping
load ../instance

@test "io shaping engine and stats" {
  # --- Basic Engine & Stats Tests ---
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping ls
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping ls --window 5
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping ls --window 60 --sys
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping ls --window 300 --json
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping disable
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping enable
}

@test "io shaping policy for an application" {
  # --- Application Policy Tests ---
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --app test --limit-write 5M
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --app test --limit-write 10M --disable
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --app test --enable
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --app test --limit-write 10M --limit-read 5M
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --app test --reservation-write 2M --reservation-read 1M
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --app test --disable
}

@test "io shaping policy for a uid" {
  # --- User (UID) Policy Tests ---
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --uid 1000 --limit-write 20M
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --uid 1000 --limit-write 20M --limit-read 10M
  runtest "### Traffic Shaping " unix ERROR "" eos io shaping policy set --uid 1000 --reservation-write 5M
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --uid 1000 --disable
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --uid 1000 --enable
}

@test "io shaping policy for a gid" {
  # --- Group (GID) Policy Tests ---
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --gid 2000 --limit-write 30M
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --gid 2000 --limit-write 30M --limit-read 15M
  runtest "### Traffic Shaping " unix ERROR "" eos io shaping policy set --gid 2000 --reservation-read 10M
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --gid 2000 --disable
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy set --gid 2000 --enable
}

@test "io shaping policy listing and its formats" {
  # --- Listing & Serialization Tests ---
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy ls
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy ls --json
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy ls --controller
}

@test "io shaping policy rm removes the policies" {
  # --- Teardown / Cleanup Tests ---
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy rm --app test
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy rm --uid 1000
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy rm --gid 2000
  runtest "### Traffic Shaping " unix SUCCESS "" eos io shaping policy ls
}
