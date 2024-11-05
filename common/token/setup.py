from setuptools import setup, Extension
import os

# Set the name of your library without the "lib" prefix or extension
# For example, "scitoken" will refer to "libscitoken.so" on Linux/macOS or "scitoken.dll" on Windows
library_name = "EosCommon"

# Set the path to the directory containing the shared library, if it's not in a standard location
library_dir = "/usr/lib64/"  # Update this to the directory with libscitoken.so or scitoken.dll

module = Extension(
    'eosscitoken',
    sources=['eosscitokenmodule.c'],  # Only the wrapper C extension file
    libraries=[library_name],       # Link against the shared library
    library_dirs=[library_dir],     # Directory where the library is located
)

setup(
    name='eosscitoken',
    version='1.0',
    description='Python interface for EOS SciToken C library',
    ext_modules=[module],
)
