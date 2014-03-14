
include ../../Make-hdb++.in

CXXFLAGS += -Wall -DRELEASE='"N$Name:  $ "' -I$(SQLIMPL_INC) -I$(TANGO_INC) -I$(OMNI_INC) -I$(LIBHDB_INC)
CXX = g++


##############################################
# support for shared libray versioning
#
LFLAGS_SONAME = -l$(SQLIMPL) -L$(SQLIMPL_LIB) -Wl,-soname,
SHLDFLAGS = -shared
BASELIBNAME       =  libhdb++mysql
SHLIB_SUFFIX = so

#  release numbers for libraries
#
 LIBVERSION    = 1
 LIBRELEASE    = 0
 LIBSUBRELEASE = 0
#

LIBRARY       = $(BASELIBNAME).a
DT_SONAME     = $(BASELIBNAME).$(SHLIB_SUFFIX).$(LIBVERSION)
DT_SHLIB      = $(BASELIBNAME).$(SHLIB_SUFFIX).$(LIBVERSION).$(LIBRELEASE).$(LIBSUBRELEASE)
SHLIB         = $(BASELIBNAME).$(SHLIB_SUFFIX)



.PHONY : install clean

lib/LibHdb++MySQL: lib obj obj/LibHdb++MySQL.o
	$(CXX) obj/LibHdb++MySQL.o $(SHLDFLAGS) $(LFLAGS_SONAME)$(DT_SONAME) -o lib/$(DT_SHLIB)
	ln -sf $(DT_SHLIB) lib/$(SHLIB)
	ln -sf $(SHLIB) lib/$(DT_SONAME)
	ar rcs lib/$(LIBRARY) obj/LibHdb++MySQL.o

obj/LibHdb++MySQL.o: src/LibHdb++MySQL.cpp src/LibHdb++MySQL.h $(LIBHDB_INC)/LibHdb++.h
	$(CXX) $(CXXFLAGS) -fPIC -c src/LibHdb++MySQL.cpp -o $@

clean:
	rm -f obj/*.o lib/*.so* lib/*.a

lib obj:
	@mkdir $@
	
	
