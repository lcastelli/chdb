PHP_ARG_WITH(chdb, whether to enable chdb support,
[ --with-chdb              Include chdb support])

PHP_ARG_WITH(libcmph-dir, for chdb,
[ --with-libcmph-dir[=DIR] Set the path to libcmph install prefix.], yes)

if test "$PHP_CHDB" != "no"; then
  SEARCH_PATH="/usr/local /usr"
  SEARCH_FOR="/include/cmph.h"
  if test -r $PHP_LIBCMPH-DIR/$SEARCH_FOR; then
    CMPH_DIR=$PHP_LIBCMPH-DIR
  else
    AC_MSG_CHECKING([for cmph files in default path])
    for i in $SEARCH_PATH ; do
      if test -r $i/$SEARCH_FOR; then
        CMPH_DIR=$i
        AC_MSG_RESULT(found in $i)
      fi
    done
  fi

  if test -z "$CMPH_DIR"; then
    AC_MSG_RESULT([not found])
    AC_MSG_ERROR([Cannot find cmph header (http://cmph.sourceforge.net/).])
  fi

  PHP_ADD_INCLUDE($CMPH_DIR/include)

  LIBNAME=cmph
  LIBSYMBOL=cmph_new
  PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,,[
    AC_MSG_ERROR([Cannot find cmph library (http://cmph.sourceforge.net/).])
  ],[
    -L$CMPH_DIR/lib -lm
  ])

  PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $CHDB_DIR/lib, CHDB_SHARED_LIBADD)
  PHP_SUBST(CHDB_SHARED_LIBADD)

  AC_C_BIGENDIAN()
  PHP_NEW_EXTENSION(chdb, chdb.c php_chdb.c, $ext_shared)
fi

