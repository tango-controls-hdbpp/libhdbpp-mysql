# liphdbpp-mysql

[![TangoControls](https://img.shields.io/badge/-Tango--Controls-7ABB45.svg?style=flat&logo=%20data%3Aimage%2Fpng%3Bbase64%2CiVBORw0KGgoAAAANSUhEUgAAACAAAAAkCAYAAADo6zjiAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAALEwAACxMBAJqcGAAAAsFJREFUWIXtl01IFVEYht9zU%2FvTqOxShLowlOgHykWUGEjUKqiocB1FQURB0KJaRdGiaFM7gzZRLWpTq2olhNQyCtpYCP1gNyIoUTFNnxZzRs8dzvw4Q6564XLnfOf73vedc2a%2BmZEKALgHrC3CUUR8CxZFeEoFalsdM4uLmMgFoIlZLJp3A9ZE4S2oKehhlaR1BTnyg2ocnW%2FxsxEDhbYij4EPVncaeASMAavnS%2FwA8NMaqACNQCew3f4as3KZOYh2SuqTVJeQNiFpn6QGSRVjTH9W%2FiThvcCn6H6n4BvQDvQWFT%2BSIDIFDAKfE3KOAQeBfB0XGPeQvgE67P8ZoB44DvTHmFgJdOQRv%2BUjc%2BavA9siNTWemgfA3TwGquCZ3w8szFIL1ALngIZorndvgJOR0GlP2gtJkzH%2Bd0fGFxW07NqY%2FCrx5QRXcYjbCbmxF1dkBSbi8kpACah3Yi2Sys74cVyxMWY6bk5BTwgRe%2BYlSzLmxNpU3aBeJogk4XWWpJKUeiap3RJYCpQj4QWZDQCuyIAk19Auj%2BAFYGZZjTGjksaBESB8P9iaxUBIaJzjZcCQcwHdj%2BS2Al0xPOeBYYKHk4vfmQ3Y8YkIwRUb7wQGU7j2ePrA1URx93ayd8UpD8klyPbSQfCOMIO05MbI%2BDvwBbjsMdGTwlX21AAMZzEerkaI9zFkP4AeYCPBg6gNuEb6I%2FthFgN1KSQupqzoRELOSed4DGiJala1UmOMr2U%2Bl%2FTWEy9Japa%2Fy41IWi%2FJ3d4%2FkkaAw0Bz3AocArqApwTvet3O3GbgV8qqjAM7bf4N4KMztwTodcYVyelywKSCD5V3xphNXoezuTskNSl4bgxJ6jPGVJJqbN0aSV%2Bd0M0aO7FCs19Jo2lExphXaTkxdRVgQFK7DZVDZ8%2BcpdmQh3wuILh7ut3AEyt%2B51%2BL%2F0cUfwFOX0t0StltmQAAAABJRU5ErkJggg%3D%3D)](http://www.tango-controls.org) [![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0) [![GitHub release](https://img.shields.io/github/release/tango-controls-hdbpp/libhdbpp-mysql.svg)](https://github.com/tango-controls-hdbpp/libhdbpp-mysql/releases)   [![Download](https://api.bintray.com/packages/tango-controls/debian/libhdb%2B%2Bmysql6/images/download.svg)](https://bintray.com/tango-controls/debian/libhdb%2B%2Bmysql6/_latestVersion)



Library for HDB++ implementing MySQL schema

## Version

The current release version is 1.0.1

## Documentation

* See the Tango documentation [here](http://tango-controls.readthedocs.io/en/latest/administration/services/hdbpp/index.html#hdb-an-archiving-historian-service) for broader information about the HDB++ archiving system and its integration into Tango Controls
* libhdbpp-mysql [CHANGELOG.md](https://github.com/tango-controls-hdbpp/libhdbpp/blob/master/CHANGELOG.md) contains the latest changes both released and in development.

## Building

### Dependencies

Ensure the development version of the dependencies are installed. These are as follows:

* HDB++ library libhdbpp
* Tango Controls 9 or higher - either via debian package or source install.
* omniORB release 4 - libomniorb4 and libomnithread.
* libzmq - libzmq3-dev or libzmq5-dev.

### Build Flags

There are a set of library and include variables that can be set to inform the build of various dependencies. The flags are only required if you have installed a dependency in a non-standard location. 

| Flag | Notes |
|------|-------|
| TANGO_INC | Tango include files directory |
| TANGO_LIB | Tango lib files directory |
| OMNIORB_INC | Omniorb include files directory |
| OMNIORB_LIB | Omniorb lib files directory |
| ZMQ_INC | ZMQ include files directory |
| LIBHDBPP_INC | Libhdb++ include files directory |
| LIBHDBPP_LIB | Libhdb++ lib files directory |

### Build

To get the source, pull from git:

```bash
git clone http://github.com/tango-controls-hdbpp/libhdbpp-mysql.git  
cd libhdbpp-mysql
```

Set appropriate flags in the environment (or pass them to make) if required, then:

```bash
make
```

## Installation

Once built simply run `make install`. The install can be passed a PREFIX variable, this is set to /usr/local by default. It is also possible to use DESTDIR. Install path is constructed as DESTDIR/PREFIX.

#### Building Against Tango Controls 9.2.5a

**The debian package and source install place the headers under /usr/include/tango, so its likely you will need to set TANGO_INC=/usr/include/tango or TANGO_INC=/usr/local/include/tango, depending on your install method.**

## DB Schema

Two files are provided to create the DB schema:

* etc/create_hdb++_mysql.sql
* etc/create_hdb++_mysql_innodb.sql

In the first file MyISAM is used as DB engine and 3 TIMESTAMP columns (data_time, recv_time, insert_time) are created for every data table.
MyIsam is deprecated and partitioning on MyIsam tables is no more supported since Mysql 8.
In the second file InnoDB is used as DB engine and 2 DATETIME columns (data_time, recv_time) are created for every data table.
A primary key (att_conf_id, data_time) is created for every data table, with this key maximum performances are obtained by partitioning tables by range on data_time column.
At the same time with this primary key duplicated of data_time with the same att_conf_id are not allowed. To ignore primary key duplication errors, the 'ignore_duplicates' configuration key can be set to 1.

### Partition management

Partitions defined in etc/create_hdb++_mysql_innodb.sql should be adjusted depending on the amount of data really present in the tables. Partitions 'p000' and 'future' should always be present to catch events arriving with wrong (in the past or in the future) timestamps.
Older partitions could be dropped with the 'DROP PARTITION' command or moved to another database with the 'ALTER TABLE ... EXCHANGE PARTITION' command.
When approaching to the end of the range of the current partition, 'future' partition should be splitted in two parts to add the new partition with the 'ALTER TABLE ... REORGANIZE PARTITION' command.

### MyIsam to InnoDB schema migration

To migrate from the schema using MyIsam to the schema wit InnoDB and partitiong the suggested sequence is the following:

* rename existing tables (e.g. 'RENAME TABLE att_scalar_devboolean_ro TO att_scalar_devboolean_ro_old;')
* dump to file renamed tables (e.g. 'SELECT ad.att_conf_id,ad.data_time,ad.recv_time, ad.value_r, ad.quality, ad.att_error_desc_id INTO OUTFILE '/tmp/att_scalar_devboolean_ro.csv' FROM hdbpp.att_scalar_devboolean_ro ad;')
* optimize MySQL configuration for InnoDB adjusting key_buffer_size, innodb_buffer_pool_size and many more parameters
* create new schema with 'etc/create_hdb++_mysql_innodb.sql'
* bulk load data from files into created tables (e.g. 'LOAD DATA LOCAL INFILE '/tmp/att_scalar_devboolean_ro.csv' INTO TABLE att_scalar_devboolean_ro;'). If the table size is big (in the order of tens of million rows) file to be loaded should be splitted into chunks of around 1 million rows: this can be easily done with scripts like 'pt-fifo-split' from Percona Tools.
* if everything went OK, old renamed tables can be dropped (e.g. 'DROP TABLE att_scalar_devboolean_ro_old;')

## License

The code is released under the LGPL3 license and a copy of this license is provided with the code. 
