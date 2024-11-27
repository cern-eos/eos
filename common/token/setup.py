from setuptools import setup, Extension
import distutils.command.build
import os

# Set the name of your library without the "lib" prefix or extension
# For example, "scitoken" will refer to "libscitoken.so" on Linux/macOS or "scitoken.dll" on Windows
library_name = "EosCommon"

destdir = os.environ.get('DESTDIR', '')
custom_build_base = os.environ.get('BUILD_BASE', '')
custom_source_base = os.environ.get("SOURCE_BASE", "")
source = "".join([custom_source_base, "eosscitokenmodule.c"])

print("info: Using custom_build_base=\"{}\"".format(custom_build_base))
print("info: Using custom_source_base=\"{}\"".format(custom_source_base))

# Override build command to set a custom build_base as destination for the
# build artifacts.
class BuildCommand(distutils.command.build.build):
    def initialize_options(self):
        distutils.command.build.build.initialize_options(self)
        self.build_base = custom_build_base

# Set the path to the directory containing the shared library, if it's not in a standard location
library_dir = f"{destdir}/usr/lib64/"

module = Extension(
    'eosscitoken',
    sources=[source],               # Only the wrapper C extension file
    libraries=[library_name],       # Link against the shared library
    library_dirs=[library_dir],     # Directory where the library is located
)

setup(
    name='eosscitoken',
    version='1.0',
    description='Python interface for EOS SciToken C library',
    ext_modules=[module],
    cmdclass={"build": BuildCommand},
)
