# Documentation: https://docs.brew.sh/Formula-Cookbook
#                https://rubydoc.brew.sh/Formula
# PLEASE REMOVE ALL GENERATED COMMENTS BEFORE SUBMITTING YOUR PULL REQUEST!
class Eos < Formula
  desc "CERN storage technology used at the Large Hadron Collider (LHC)"
  homepage "https://eos-web.web.cern.ch/eos-web/"
  url "https://gitlab.cern.ch/dss/eos.git", branch: "brew"
  version "5.0.31"
  license ""
  head "https://gitlab.cern.ch/dss/eos.git", branch: "brew"

  depends_on "cmake" => :build
  depends_on "xrootd" => :build
  depends_on "openssl" => :build
  depends_on "protobuf@3" => :build
  depends_on "readline" => :build
  depends_on "ncurses" => :build
  depends_on "zlib" => :build
  depends_on "ossp-uuid" => :build
  depends_on "google-sparsehash" => :build
  depends_on "leveldb" => :build
  
  depends_on "jsoncpp" => :build
  depends_on "lz4" => :build
  depends_on "openssl@3" => :build
  depends_on "protobuf@3" => :build
  depends_on "zeromq" => :build
  depends_on "zlib" => :build
  depends_on "zstd" => :build
	
  def install
    # ENV.deparallelize  # if your formula fails when building in parallel
    # Remove unrecognized options if warned by configure
    # https://rubydoc.brew.sh/Formula.html#std_configure_args-instance_method
    #system "./configure", *std_configure_args, "--disable-silent-rules"
    mkdir "build" do
        system "cmake", "../", *std_cmake_args, "-DCLIENT=1", "-C/Users/manuelreis/repos/eos/cmake/my_vars_new.cmake"
        system "make", "install", "-j", "8"
    end

    # system "cmake", "-S", ".", "-B", "build", *std_cmake_args
  end
  test do
    # `test do` will create, run in and delete a temporary directory.
    #
    # This test will fail and we won't accept that! For Homebrew/homebrew-core
    # this will need to be a test that verifies the functionality of the
    # software. Run the test with `brew test eos`. Options passed
    # to `brew install` such as `--HEAD` also need to be provided to `brew test`.
    #
    # The installed folder is not in the path, so use the entire path to any
    # executables being tested: `system "#{bin}/program", "do", "something"`.
    system "eos", "--version"
    system "eosxd", "--help"
  end
end