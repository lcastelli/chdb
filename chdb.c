#include "chdb.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <cmph.h>

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#define CHDB_FILE_MAGIC htonl('c' << 24 | 'h' << 16 | 'd' <<  8 | 'b')

#ifdef WORDS_BIGENDIAN
  /* Flag the big endian version with 1 bit to signal incompatibility */
# define CHDB_FILE_VERSION (1u << 31 | CHDB_VERSION)
#else
# define CHDB_FILE_VERSION htonl(CHDB_VERSION)
#endif

/* Align entries on 4-byte boundaries */
#define CHDB_ALIGN_ORDER 2

/**
 * Computes the first address of memory naturally aligned for the given
 * 'align_order' starting from 'address'.
 */
static inline size_t mem_align(size_t address, int align_order) {
	size_t mask = (1 << align_order) - 1;
	return (address + mask) & ~mask;
}

/**
 * The header of a chdb file.
 * - magic: chdb file magic number
 * - version: version of the file and endianness
 * - file_size: total size of the file in bytes
 * - table_offset: offset of the entry table from the beginning of the file
 * - entry_count: number of key-value pairs
 * - mph: packed cmph hashing function
 */
struct __chdb {
	uint32_t magic;
	uint32_t version;
	uint64_t file_size;
	uint32_t table_offset;
	uint32_t entry_count;
	char mph[];
} __attribute__((packed));

/**
 * Gets the entry table for a memory-mapped chdb file.
 * The entry table is an array of uint32_t with the offsets of
 * the chdb_entries from the beginning of the file indexed by
 * the hash of their keys. The offsets are shifted right by the
 * align order, so that we can effectively address 2^(32+align_order)
 * bytes of memory with 32 bit values - that is 16 GiB of memory.
 */
static inline uint32_t *chdb_get_table(chdb_t *chdb)
{
	return (uint32_t *)((char *)chdb + chdb->table_offset);
}

static struct chdb_entry *chdb_get_entry(chdb_t *chdb, uint32_t idx)
{
	uint32_t offset;
	if (idx >= chdb->entry_count)
		return NULL;

	offset = chdb_get_table(chdb)[idx];
	return (struct chdb_entry *)((char *)chdb +
	                             (offset << CHDB_ALIGN_ORDER));
}

/**
 * A key-value pair stored in a chdb file.
 * The key is stored just after the value. This layout was chosen
 * so that key_len, value_len and value are always aligned on 4-byte
 * boundaries. The key instead is not necessarily aligned, but it's
 * only used for a memcmp when doing a lookup.
 */
struct chdb_entry {
	uint32_t key_len;
	uint32_t value_len;
	char value[];
} __attribute__((packed));

static inline void *chdb_entry_get_key(struct chdb_entry *entry)
{
	return entry->value + entry->value_len;
}

/**
 * Reads the chdb header and does some compatibility checks.
 * Sets the declared file size and returns 0 on success, or returns
 * an error code otherwise.
 */
static int chdb_read_header(int fd, uint64_t *file_size)
{
	struct __chdb chdb;
	size_t rd = 0;

	do {
		int curr_rd = read(fd, (char*)&chdb + rd, sizeof(chdb) - rd);
		if (curr_rd > 0)
			rd += curr_rd;
		else if (curr_rd == 0)
			return EINVAL;
		else if (errno != EINTR)
			return errno;
	} while (rd < sizeof(chdb));

	if (chdb.magic != CHDB_FILE_MAGIC || chdb.version != CHDB_FILE_VERSION
	    /* check if the file is too big to mmap on this architecture */
	    || chdb.file_size > SIZE_MAX)
		return EINVAL;

	*file_size = chdb.file_size;
	return 0;
}

chdb_t *chdb_open(const char* pathname)
{
	int fd, _errno = 0;
	uint64_t size = 0;
	chdb_t *chdb = NULL;

	if ((fd = open(pathname, O_RDONLY)) < 0)
		return NULL; /* errno is already set */

	if ((_errno = chdb_read_header(fd, &size)))
		goto close_fd;

	if ((chdb = mmap(NULL, (size_t)size, PROT_READ, MAP_SHARED, fd, 0))
	                == MAP_FAILED) {
		_errno = errno;
		goto close_fd;
	}

close_fd:
	close(fd);

	if (_errno)
		errno = _errno;
	return chdb;
}

void chdb_close(chdb_t *chdb)
{
	munmap(chdb, (size_t)chdb->file_size);
}

int chdb_get(chdb_t *chdb, const void *key, uint32_t key_len,
             const void **value, uint32_t *value_len)
{
	uint32_t idx;
	struct chdb_entry *entry;

	idx = cmph_search_packed(chdb->mph, key, key_len);
	entry = chdb_get_entry(chdb, idx);
	if (entry == NULL || entry->key_len != key_len ||
	    memcmp(chdb_entry_get_key(entry), key, key_len)) {
		errno = EINVAL;
		return -1;
	}

	*value = entry->value;
	*value_len = entry->value_len;
	return 0;
}

static int chdb_adapter_read(void *data, char **key, uint32_t *key_len)
{
	const void *my_key, *value;
	uint32_t my_key_len, value_len;
	struct chdb_reader *reader = (struct chdb_reader*)data;

	reader->next(reader, &my_key, &my_key_len, &value, &value_len);
	/* Apparently the key should be copied before being passed to cmph */
	if ((*key = malloc(my_key_len)) == NULL) {
		key = NULL;
		key_len = 0;
		return 0;
	}
	memcpy(*key, my_key, my_key_len);
	*key_len = my_key_len;
	return my_key_len;
}

static void chdb_adapter_dispose(void *data, char *key, uint32_t key_len)
{
	free(key);
}

static void chdb_adapter_rewind(void *data)
{
	struct chdb_reader *reader = (struct chdb_reader*)data;
	reader->rewind(reader);
}

/**
 * Generates a perfect hash function for the data provided by 'reader'.
 * Sets 'mph' to point to the function and returns 0, or returns
 * an error code otherwise.
 */
static int chdb_generate_hash(struct chdb_reader *reader, cmph_t **mph)
{
	cmph_config_t *config;
	cmph_io_adapter_t adapter = {
		.data = reader,
		.nkeys = reader->count,
		.read = chdb_adapter_read,
		.dispose = chdb_adapter_dispose,
		.rewind = chdb_adapter_rewind
	};

	/* cmph hangs in case of 0 keys, so just return an error here */
	if (adapter.nkeys == 0)
		return EINVAL;

	if ((config = cmph_config_new(&adapter)) == NULL)
		return ENOMEM;

	cmph_config_set_algo(config, CMPH_CHD);
	*mph = cmph_new(config);
	cmph_config_destroy(config);

	return *mph != NULL ? 0 : ENOMEM;
}

/**
 * Writes a chdb file with the data provided by 'reader' and the hash
 * function 'mph'.
 * Returns 0 in case of success, or an error code otherwise.
 */
static int chdb_serialize(struct chdb_reader *reader, cmph_t *mph, FILE *out)
{
	struct __chdb chdb;
	struct chdb_entry entry;
	uint32_t packed_size, hash, sh_pos;
	char *packed_mph;
	const void *key, *value;
	size_t written, table_offset;
	int i;
	long pos;

	if ((packed_size = cmph_packed_size(mph)) == 0 ||
	    (table_offset = mem_align(sizeof(chdb) + packed_size,
	                              CHDB_ALIGN_ORDER)) > UINT32_MAX)
		return EINVAL;

	/* Write header */
	chdb.magic = CHDB_FILE_MAGIC;
	chdb.version = CHDB_FILE_VERSION;
	chdb.table_offset = (uint32_t)table_offset;
	chdb.entry_count = reader->count;
	if (fwrite(&chdb, 1, sizeof(chdb), out) < sizeof(chdb))
		return EIO;

	/* Write mph */
	if ((packed_mph = malloc(packed_size)) == NULL)
		return ENOMEM;

	cmph_pack(mph, packed_mph);
	written = fwrite(packed_mph, 1, packed_size, out);
	free(packed_mph);
	if (written < packed_size)
		return EIO;

	/* Skip the entry table */
	if (fseek(out, table_offset + chdb.entry_count * sizeof(uint32_t),
	          SEEK_SET))
		return errno;

	/* Write entries and fill the entry table */
	reader->rewind(reader);
	for (i = 0; i < chdb.entry_count; ++i) {
		reader->next(reader, &key, &entry.key_len,
		             &value, &entry.value_len);
		hash = cmph_search(mph, key, entry.key_len);

		/* Put the current (aligned) offset in the entry table */
		if ((pos = ftell(out)) == 0)
			return errno;
		pos = mem_align(pos, CHDB_ALIGN_ORDER);
		if ((pos >> CHDB_ALIGN_ORDER) > UINT32_MAX)
			return EINVAL;
		sh_pos = (uint32_t)(pos >> CHDB_ALIGN_ORDER);
		if (fseek(out, table_offset + hash * sizeof(uint32_t), SEEK_SET))
			return errno;
		if (fwrite(&sh_pos, 1, sizeof(sh_pos), out) < sizeof(sh_pos))
			return EIO;
		if (fseek(out, pos, SEEK_SET))
			return errno;

		/* Write the entry */
		if (fwrite(&entry, 1, sizeof(entry), out) < sizeof(entry) ||
		    fwrite(value, 1, entry.value_len, out) < entry.value_len ||
		    fwrite(key, 1, entry.key_len, out) < entry.key_len)
			return EIO;
	}

	/* Write the file size */
	if ((chdb.file_size = ftell(out)) == 0 ||
	    fseek(out, (char *)&chdb.file_size - (char *)&chdb, SEEK_SET))
		return errno;
	if (fwrite(&chdb.file_size, 1, sizeof(chdb.file_size), out)
	                                        < sizeof(chdb.file_size))
		return EIO;

	return 0;
}

int chdb_create(struct chdb_reader *reader, const char *pathname)
{
	int _errno = 0;
	cmph_t *mph;
	FILE *out;

	if ((_errno = chdb_generate_hash(reader, &mph)))
		goto return_error;

	if ((out = fopen(pathname, "w")) == NULL) {
		_errno = errno;
		goto free_cmph;
	}

	_errno = chdb_serialize(reader, mph, out);

	if (fclose(out) == EOF && _errno == 0)
		_errno = errno;

	if (_errno)
		remove(pathname); /* Don't leave invalid files behind */

free_cmph:
	cmph_destroy(mph);

return_error:
	if (_errno) {
		errno = _errno;
		return -1;
	} else
		return 0;
}

