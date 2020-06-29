PREFIX=/usr/local

DBIMPL_INC = `mysql_config --include`
DBIMPL_LIB = `mysql_config --libs_r`

ifdef TANGO_INC
	INC_DIR += -I${TANGO_INC}
endif

ifdef TANGO_LIB
	LIB_DIR	+= -L${TANGO_LIB}
endif

ifdef OMNIORB_INC
	INC_DIR	+= -I${OMNIORB_INC}
endif

ifdef OMNIORB_LIB
	LIB_DIR	+= -L${OMNIORB_LIB}
endif

ifdef ZMQ_INC
	INC_DIR += -I${ZMQ_INC}
endif

ifdef LIBHDBPP_INC
	INC_DIR += -I${LIBHDBPP_INC}
endif

CXXFLAGS += -std=gnu++0x -Wall -DRELEASE='"$HeadURL$ "' $(DBIMPL_INC) $(INC_DIR)
LDFLAGS += $(LIB_DIR) -ltango -lomnithread -lomniORB4

##############################################
# support for shared libray versioning
#
LFLAGS_SONAME = $(DBIMPL_LIB) $(LDFLAGS) -Wl,-z,defs,-soname,
SHLDFLAGS = -shared
BASELIBNAME       =  libhdb++mysql
SHLIB_SUFFIX = so

#  release numbers for libraries
#
 LIBVERSION    = 6
 LIBRELEASE    = 2
 LIBSUBRELEASE = 2
#

LIBRARY       = $(BASELIBNAME).a
DT_SONAME     = $(BASELIBNAME).$(SHLIB_SUFFIX).$(LIBVERSION)
DT_SHLIB      = $(BASELIBNAME).$(SHLIB_SUFFIX).$(LIBVERSION).$(LIBRELEASE).$(LIBSUBRELEASE)
SHLIB         = $(BASELIBNAME).$(SHLIB_SUFFIX)

.PHONY : install clean

lib/LibHdb++MySQL: lib obj obj/LibHdb++MySQL.o
	$(CXX) obj/LibHdb++MySQL.o $(SHLDFLAGS) $(LFLAGS_SONAME)$(DT_SONAME) -o lib/$(DT_SHLIB)
	ln -sf $(DT_SHLIB) lib/$(DT_SONAME)
	ln -sf $(DT_SONAME) lib/$(SHLIB)
	ar rcs lib/$(LIBRARY) obj/LibHdb++MySQL.o

obj/LibHdb++MySQL.o: src/LibHdb++MySQL.cpp src/LibHdb++MySQL.h
	$(CXX) $(CXXFLAGS) -fPIC -c src/LibHdb++MySQL.cpp -o $@

clean:
	rm -f obj/*.o lib/*.so* lib/*.a

lib obj:
	@mkdir $@
	
install:
	install -d ${DESTDIR}${PREFIX}/lib
	install -d ${DESTDIR}${PREFIX}/share/libhdb++mysql
	install -m 755 lib/libhdb++mysql.so.${LIBVERSION}.${LIBRELEASE}.${LIBSUBRELEASE} ${DESTDIR}${PREFIX}/lib	
	ln -s ${DESTDIR}${PREFIX}/lib/libhdb++mysql.so.${LIBVERSION}.${LIBRELEASE}.${LIBSUBRELEASE} ${DESTDIR}${PREFIX}/lib/libhdb++mysql.so.${LIBVERSION}
	install -m 644 etc/add_devenum__hdb++_mysql.sql ${DESTDIR}${PREFIX}/share/libhdb++mysql
	install -m 644 etc/create_hdb++_mysql_delete_attr_procedure.sql ${DESTDIR}${PREFIX}/share/libhdb++mysql
	install -m 644 etc/create_hdb++_mysql.sql ${DESTDIR}${PREFIX}/share/libhdb++mysql		

	
