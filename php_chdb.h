/* This module exposes to PHP the equivalent of:
 *   // Creates a chdb file containing the key-value pairs specified in the
 *   // array $data, or throws an exception in case of error.
 *   function chdb_create($pathname, $data);
 *
 *   // Represents a loaded chdb file.
 *   class chdb
 *   {
 *           // Loads a chdb file, or throws an exception in case of error.
 *           public function __construct($pathname);
 *
 *           // Returns the value of the given $key, or null if not found.
 *           public function get($key);
 *   }
 */

#ifndef PHP_CHDB_H
#define PHP_CHDB_H

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <php.h>

#define PHP_CHDB_VERSION "0.1"

extern zend_module_entry chdb_module_entry;
#define phpext_chdb_ptr &chdb_module_entry

#endif

