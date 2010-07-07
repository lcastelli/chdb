#include "php_chdb.h"

#ifdef ZTS
# include <TSRM.h>
#endif

#include <php_ini.h>
#include <ext/standard/info.h>
#include <stdio.h>
#include <string.h>
#include "chdb.h"

#ifdef COMPILE_DL_CHDB
ZEND_GET_MODULE(chdb)
#endif


static void throw_except_errno(char *format, char *arg, int _errno TSRMLS_DC)
{
	char buf[0x100];
	if (strerror_r(_errno, buf, 0x100))
		sprintf(buf, "Undefined error %d", _errno);
	zend_throw_exception_ex(zend_exception_get_default(TSRMLS_C),
	                        _errno TSRMLS_CC, format, arg, buf);
}


ZEND_BEGIN_MODULE_GLOBALS(php_chdb)
	HashTable chdb_list;
ZEND_END_MODULE_GLOBALS(php_chdb)

ZEND_DECLARE_MODULE_GLOBALS(php_chdb)

#ifdef ZTS
# define CHDB_G(v) TSRMG(php_chdb_globals_id, zend_php_chdb_globals *, v)
#else
# define CHDB_G(v) (php_chdb_globals.v)
#endif

static void php_chdb_destroy_list_entry(chdb_t **entry)
{
	chdb_close(*entry);
}

static void php_chdb_init_globals(zend_php_chdb_globals *globals TSRMLS_DC)
{
	zend_hash_init(&globals->chdb_list, 16, NULL,
	               (dtor_func_t)php_chdb_destroy_list_entry, 1);
}

static void php_chdb_destroy_globals(zend_php_chdb_globals *globals TSRMLS_DC)
{
	zend_hash_destroy(&globals->chdb_list);
}


#define KEY_BUFFER_LEN 21
struct php_chdb_reader_private {
	HashTable *data;
	HashPosition pos;
	zval val_copy;
	char key_buffer[KEY_BUFFER_LEN];
};

static void php_chdb_reader_next(struct chdb_reader *reader,
                                 const void **key, uint32_t *key_len,
                                 const void **value, uint32_t *value_len)
{
	char *my_key;
	uint my_key_len;
	long idx;
	zval **cur;
	struct php_chdb_reader_private *private = reader->private;

	if (zend_hash_get_current_key_ex(private->data, &my_key, &my_key_len,
	                        &idx, 0, &private->pos) == HASH_KEY_IS_LONG) {
		/* convert the key to string */
		my_key_len = snprintf(private->key_buffer, KEY_BUFFER_LEN, "%ld", idx);
		my_key = private->key_buffer;
	} else {
		/* ignore NULL string terminator */
		my_key_len--;
	}

	/* convert the value to string */
	zend_hash_get_current_data_ex(private->data, (void **)&cur, &private->pos);
	zval_dtor(&private->val_copy); /* delete the last copy */
	private->val_copy = **cur;
	zval_copy_ctor(&private->val_copy);
	INIT_PZVAL(&private->val_copy);
	convert_to_string(&private->val_copy);

	*key = my_key;
	*key_len = my_key_len;
	*value = Z_STRVAL(private->val_copy);
	*value_len = Z_STRLEN(private->val_copy);

	zend_hash_move_forward_ex(private->data, &private->pos);
}

static void php_chdb_reader_rewind(struct chdb_reader *reader)
{
	struct php_chdb_reader_private *private = reader->private;
	zend_hash_internal_pointer_reset_ex(private->data, &private->pos);
}

ZEND_BEGIN_ARG_INFO_EX(php_chdb_arginfo_create, 0, 0, 2)
	ZEND_ARG_INFO(0, pathname)
	ZEND_ARG_ARRAY_INFO(0, data, 0)
ZEND_END_ARG_INFO()

static PHP_FUNCTION(chdb_create)
{
	char *pathname;
	uint pathname_len;
	zval *data;
	struct chdb_reader reader;
	struct php_chdb_reader_private private;
	int _errno = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sa",
	                          &pathname, &pathname_len, &data) == FAILURE) {
		zend_throw_exception_ex(zend_exception_get_default(TSRMLS_C),
		                        0 TSRMLS_CC, "Invalid parameters");
		RETURN_FALSE;
	}

	private.data = Z_ARRVAL_P(data);
	zend_hash_internal_pointer_reset_ex(private.data, &private.pos);
	INIT_ZVAL(private.val_copy);

	reader.private = &private;
	reader.count = zend_hash_num_elements(private.data);
	reader.next = php_chdb_reader_next;
	reader.rewind = php_chdb_reader_rewind;

	if (chdb_create(&reader, pathname))
		_errno = errno;

	zval_dtor(&private.val_copy);

	if (_errno) {
		throw_except_errno("Error generating '%s': %s", pathname,
		                   _errno TSRMLS_CC);
		RETURN_FALSE;
	}

	RETURN_TRUE;
}

const zend_function_entry php_chdb_functions[] = {
	PHP_FE(chdb_create, php_chdb_arginfo_create)
	{ }
};


static zend_class_entry *php_chdb_ce;

struct php_chdb {
	zend_object zo;
	chdb_t *chdb;
};

static void php_chdb_free_storage(struct php_chdb *intern TSRMLS_DC)
{
	zend_object_std_dtor(&intern->zo TSRMLS_CC);
	efree(intern);
}

static zend_object_value php_chdb_new(zend_class_entry *ce TSRMLS_DC)
{
	zend_object_value retval;
	struct php_chdb *intern;
	zval *tmp;

	intern = ecalloc(1, sizeof(*intern));
	zend_object_std_init(&intern->zo, ce TSRMLS_CC);
	zend_hash_copy(intern->zo.properties, &ce->default_properties,
	         (copy_ctor_func_t)zval_add_ref, (void *)&tmp, sizeof(zval *));

	retval.handle = zend_objects_store_put(intern, NULL,
	          (zend_objects_free_object_storage_t)php_chdb_free_storage,
	          NULL TSRMLS_CC);
	retval.handlers = zend_get_std_object_handlers();

	return retval;
}

ZEND_BEGIN_ARG_INFO_EX(php_chdb_arginfo___construct, 0, 0, 1)
	ZEND_ARG_INFO(0, pathname)
ZEND_END_ARG_INFO()

static PHP_METHOD(chdb, __construct)
{
	char *pathname;
	uint pathname_len;
	ulong pathname_hash;
	chdb_t *chdb, **lookup;
	zval *object = getThis();
	struct php_chdb *intern = (struct php_chdb *)
	                 zend_object_store_get_object(object TSRMLS_CC);

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
	                          &pathname, &pathname_len) == FAILURE) {
		zend_throw_exception_ex(zend_exception_get_default(TSRMLS_C),
		                        0 TSRMLS_CC, "Invalid parameters");
		RETURN_FALSE;
	}

	pathname_hash = zend_get_hash_value(pathname, pathname_len);
	if (zend_hash_quick_find(&CHDB_G(chdb_list), pathname, pathname_len,
	                         pathname_hash, (void **)&lookup) == SUCCESS) {
		intern->chdb = *lookup;
	} else {
		if ((chdb = chdb_open(pathname)) == NULL) {
			throw_except_errno("Cannot load '%s': %s", pathname,
			                   errno TSRMLS_CC);
			RETURN_FALSE;
		}

		zend_hash_quick_add(&CHDB_G(chdb_list), pathname, pathname_len,
		                    pathname_hash, &chdb, sizeof(chdb), NULL);
		intern->chdb = chdb;
	}
}

ZEND_BEGIN_ARG_INFO_EX(php_chdb_arginfo_get, 0, 0, 1)
	ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()

static PHP_METHOD(chdb, get)
{
	char *key, *value;
	uint key_len, value_len;
	zval *object = getThis();
	struct php_chdb *intern = (struct php_chdb *)
                         zend_object_store_get_object(object TSRMLS_CC);

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
	                          &key, &key_len) == FAILURE) {
		zend_throw_exception_ex(zend_exception_get_default(TSRMLS_C),
		                        0 TSRMLS_CC, "Invalid parameters");
		RETURN_NULL();
	}

	if (chdb_get(intern->chdb, key, key_len,
	             (const void **)&value, &value_len))
		RETURN_NULL();

	RETVAL_STRINGL(value, value_len, 1);
}

#define CHDB_ME(name, args) PHP_ME(chdb, name, args, ZEND_ACC_PUBLIC)
static zend_function_entry php_chdb_class_methods[] = {
	CHDB_ME(__construct, php_chdb_arginfo___construct)
	CHDB_ME(get,         php_chdb_arginfo_get)
	{ }
};


static PHP_MINIT_FUNCTION(chdb)
{
	zend_class_entry ce;

	INIT_CLASS_ENTRY(ce, "chdb", php_chdb_class_methods);
	php_chdb_ce = zend_register_internal_class(&ce TSRMLS_CC);
	php_chdb_ce->create_object = php_chdb_new;

#ifdef ZTS
	ts_allocate_id(&php_chdb_globals_id,
	               sizeof(zend_php_chdb_globals),
	               (ts_allocate_ctor)php_chdb_init_globals,
	               (ts_allocate_dtor)php_chdb_destroy_globals);
#else
	php_chdb_init_globals(&php_chdb_globals TSRMLS_CC);
#endif

	return SUCCESS;
}

static PHP_MSHUTDOWN_FUNCTION(chdb)
{
#ifdef ZTS
	ts_free_id(php_chdb_globals_id);
#else
	php_chdb_destroy_globals(&php_chdb_globals TSRMLS_CC);
#endif

	return SUCCESS;
}

static PHP_MINFO_FUNCTION(chdb)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "chdb support", "enabled");
	php_info_print_table_row(2, "version", PHP_CHDB_VERSION);
	php_info_print_table_end();
}

zend_module_entry chdb_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
	STANDARD_MODULE_HEADER,
#endif
	"chdb",
	php_chdb_functions,
	PHP_MINIT(chdb),
	PHP_MSHUTDOWN(chdb),
	NULL,	
	NULL,
	PHP_MINFO(chdb),
#if ZEND_MODULE_API_NO >= 20010901
	PHP_CHDB_VERSION,
#endif
	STANDARD_MODULE_PROPERTIES
};

