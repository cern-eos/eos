//------------------------------------------------------------------------------
// File: eosscitokenmodule.c
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/ASwitzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <Python.h>
#include <stdlib.h>

// Declare your original C functions here
extern void* c_scitoken_factory_init(const char* cred, const char* key,
                                     const char* keyid, const char* issuer);
extern int c_scitoken_create(char* token, size_t token_length, time_t validity,
                             const char* claim1, const char* claim2,
                             const char* claim3, const char* claim4);

// Wrapper for c_scitoken_factory_init
static PyObject*
py_c_scitoken_factory_init(PyObject* self, PyObject* args)
{
  const char* cred;
  const char* key;
  const char* keyid;
  const char* issuer;

  // Parse arguments from Python (all strings)
  if (!PyArg_ParseTuple(args, "ssss", &cred, &key, &keyid, &issuer)) {
    return NULL;
  }

  // Call the C function
  void* result = c_scitoken_factory_init(cred, key, keyid, issuer);

  // Return the pointer as a Python integer (or None if NULL)
  if (result == NULL) {
    Py_RETURN_NONE;
  } else {
    return PyLong_FromVoidPtr(result);
  }
}

// Wrapper for c_scitoken_create
static PyObject*
py_c_scitoken_create(PyObject* self, PyObject* args)
{
  time_t validity = 0;
  const char* claim1 = "";
  const char* claim2 = "";
  const char* claim3 = "";
  const char* claim4 = "";
  size_t token_length;

  // Parse arguments from Python
  PyObject* token_buffer_obj;
  if (!PyArg_ParseTuple(args, "O!kL|ssss", &PyByteArray_Type, &token_buffer_obj,
                        &token_length, &validity, &claim1, &claim2, &claim3,
                        &claim4)) {
    return NULL;
  }

  // Get the pointer to the token buffer
  char* token_buffer = PyByteArray_AsString(token_buffer_obj);

  // Call the C function
  int result = c_scitoken_create(token_buffer, token_length, validity, claim1,
                                 claim2, claim3, claim4);

  // Return the result as an integer
  return PyLong_FromLong(result);
}

// Define the module's methods
static PyMethodDef SciTokenMethods[] = {
    {"c_scitoken_factory_init", py_c_scitoken_factory_init, METH_VARARGS,
     "Initialize SciToken factory"},
    {"c_scitoken_create", py_c_scitoken_create, METH_VARARGS,
     "Create SciToken"},
    {NULL, NULL, 0, NULL}};

// Define the module
static struct PyModuleDef scitokenmodule = {
    PyModuleDef_HEAD_INIT,
    "eosscitoken",  // Module name
    NULL,           // Module documentation
    -1,             // Size of per-interpreter state of the module
    SciTokenMethods // Module's methods
};

// Module initialization function
PyMODINIT_FUNC
PyInit_eosscitoken(void)
{
  return PyModule_Create(&scitokenmodule);
}
