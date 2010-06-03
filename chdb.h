#ifndef CHDB_H
#define CHDB_H

#include <stdint.h>

#define CHDB_VERSION 0

typedef struct __chdb chdb_t;

/**
 * Memory-maps a chdb file.
 * Returns a handle to the opened chdb, or sets errno and returns
 * NULL in case of error.
 */
chdb_t *chdb_open(const char *pathname);

/**
 * Closes a memory-mapped chdb file.
 */
void chdb_close(chdb_t *chdb);

/**
 * Performs a search for the given 'key' of length 'key_len' in 'chdb'.
 * Sets 'value' and 'value_len' and returns 0 in case of success,
 * or returns -1 and sets errno otherwise.
 * Note that the area of memory 'value' points to after a successful
 * call to this function is read-only.
 */
int chdb_get(chdb_t *chdb, const void *key, uint32_t key_len,
             const void **value, uint32_t *value_len);

/**
 * Provides a set of key-value pairs to be indexed by chdb.
 * This structure should be filled in by the user code and passed to
 * chdb_create to generate a chdb file.
 * - private: a pointer to user data, ignored by chdb
 * - count: the number of key-value pairs to be indexed
 * - next: a pointer to the iterator function that provides the next
 *         key-value pair
 * - rewind: a pointer to a function that should rewind the reader
 *           back to the beginning of the set
 */
struct chdb_reader {
	void *private;
	uint32_t count;
	void (*next)(struct chdb_reader *reader,
	             const void **key, uint32_t *key_len,
	             const void **value, uint32_t *value_len);
	void (*rewind)(struct chdb_reader *reader);
};

/**
 * Generates a chdb file at the given 'pathname' containing the data
 * provided by 'reader'.
 * Returns 0 in case of success, or sets errno and returns -1 otherwise.
 */
int chdb_create(struct chdb_reader *reader, const char *pathname);

#endif

