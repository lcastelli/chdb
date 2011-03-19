// Include these first to ensure that we get the POSIX strerror_r
#include <stdio.h>
#include <string.h>

#include "php_chdb.h"

#ifdef ZTS
# include <TSRM.h>
#endif

#include <php_ini.h>
#include <ext/standard/info.h>
#include <Zend/zend_exceptions.h>
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
	ulong idx;
	zval **cur;
	struct php_chdb_reader_private *private = reader->private;

	if (zend_hash_get_current_key_ex(private->data, &my_key, &my_key_len,
	                        &idx, 0, &private->pos) == HASH_KEY_IS_LONG) {
		/* convert the key to string */
		my_key_len = snprintf(private->key_buffer, KEY_BUFFER_LEN,
		                      "%ld", idx);
		my_key = private->key_buffer;
	} else {
		/* ignore NULL string terminator */
		my_key_len--;
	}

	/* convert the value to string */
	zend_hash_get_current_data_ex(private->data,
	                              (void **)&cur, &private->pos);
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
	if (intern->chdb)
		chdb_close(intern->chdb);
	efree(intern);
}

static zend_object_value php_chdb_new(zend_class_entry *ce TSRMLS_DC)
{
	zend_object_value retval;
	struct php_chdb *intern;
	zval *tmp;

	intern = ecalloc(1, sizeof(*intern));
	intern->chdb = NULL;
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
	chdb_t *chdb;
	zval *object = getThis();
	struct php_chdb *intern = (struct php_chdb *)
	                 zend_object_store_get_object(object TSRMLS_CC);

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
	                          &pathname, &pathname_len) == FAILURE) {
		zend_throw_exception_ex(zend_exception_get_default(TSRMLS_C),
		                        0 TSRMLS_CC, "Invalid parameters");
		RETURN_FALSE;
	}

	if ((chdb = chdb_open(pathname)) == NULL) {
		throw_except_errno("Cannot load '%s': %s", pathname,
		                   errno TSRMLS_CC);
		RETURN_FALSE;
	}

	intern->chdb = chdb;
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
	NULL,
	NULL,	
	NULL,
	PHP_MINFO(chdb),
#if ZEND_MODULE_API_NO >= 20010901
	PHP_CHDB_VERSION,
#endif
	STANDARD_MODULE_PROPERTIES
};

