//=============================================================================
//
// file :        LibHdb++MySQL.h
//
// description : Include for the LibHdb++MySQL library.
//
// Author: Graziano Scalamera
//
// $Revision: 1.1 $
//
// $Log: LibHdb++MySQL.h,v $
// Revision 1.1  2014-03-07 13:17:22  graziano
// first commit
//
//
//
//
//=============================================================================

#ifndef _HDBPP_MYSQL_H
#define _HDBPP_MYSQL_H

#include <mysql.h>
#include "LibHdb++.h"

#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <stdint.h>

//Tango:
#include <tango.h>
//#include <event.h>

#define TYPE_SCALAR					"scalar"
#define TYPE_ARRAY					"array"
#define TYPE_DOUBLE					"double"
#define TYPE_I64					"int64"
#define TYPE_I8						"int8"
#define TYPE_STRING					"string"
#define TYPE_ENCODED				"encoded"
#define TYPE_RO						"ro"
#define TYPE_RW						"rw"

#define EVENT_ADD					"add"
#define EVENT_REMOVE				"remove"
#define EVENT_START					"start"
#define EVENT_STOP					"stop"

//######## att_conf ########
#define CONF_TABLE_NAME				"att_conf"
#define CONF_COL_ID					"att_conf_id"
#define CONF_COL_NAME				"att_name"
#define CONF_COL_TYPE				"data_type"

//######## att_history ########
#define HISTORY_TABLE_NAME			"att_history"
#define HISTORY_COL_ID				"att_conf_id"
#define HISTORY_COL_EVENT			"event"
#define HISTORY_COL_TIME			"time"

//######## att_scalar_... ########
#define SC_DOUBLE_RO_TABLE_NAME		"att_scalar_double_ro"
#define SC_I64_RO_TABLE_NAME		"att_scalar_int64_ro"
#define SC_I8_RO_TABLE_NAME			"att_scalar_int8_ro"
#define SC_STRING_RO_TABLE_NAME		"att_scalar_string_ro"
#define SC_DOUBLE_RW_TABLE_NAME		"att_scalar_double_rw"
#define SC_I64_RW_TABLE_NAME		"att_scalar_int64_rw"
#define SC_I8_RW_TABLE_NAME			"att_scalar_int8_rw"
#define SC_STRING_RW_TABLE_NAME		"att_scalar_string_rw"

#define SC_COL_ID					"att_conf_id"
#define SC_COL_INS_TIME				"insert_time"
#define SC_COL_RCV_TIME				"recv_time"
#define SC_COL_EV_TIME				"event_time"
#define SC_COL_VALUE_R				"value_r"
#define SC_COL_VALUE_W				"value_w"



//######## att_array_double_ro ########
#define ARR_DOUBLE_RO_TABLE_NAME	"att_array_double_ro"
#define ARR_DOUBLE_RW_TABLE_NAME	"att_array_double_rw"

#define ARR_COL_ID					"att_conf_id"
#define ARR_COL_INS_TIME			"insert_time"
#define ARR_COL_RCV_TIME			"recv_time"
#define ARR_COL_EV_TIME				"event_time"
#define ARR_COL_VALUE_R				"value_r"
#define ARR_COL_VALUE_W				"value_w"
#define ARR_COL_IDX					"idx"
#define ARR_COL_DIMX				"dim_x"
#define ARR_COL_DIMY				"dim_y"



class HdbPPMySQL : public AbstractDB
{
private:

	MYSQL *dbp;
	string m_dbname;
	map<string,int> attr_ID_map;
	
	string get_only_attr_name(string str);
	string get_only_tango_host(string str);
	string remove_domain(string facility);
	string add_domain(string facility);
	string get_data_type(int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/);
	string get_table_name(int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/);

public:

	~HdbPPMySQL();

	HdbPPMySQL(string host, string user, string password, string dbname, int port);

	//void connect_db(string host, string user, string password, string dbname);
	int find_attr_id(string facility, string attr_name, int &ID);
	int find_attr_id_type(string facility, string attr_name, int &ID, string attr_type);
	virtual int insert_Attr(Tango::EventData *data, HdbEventDataType ev_data_type);
	virtual int configure_Attr(string name, int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/);
	virtual int remove_Attr(string name);
	virtual int start_Attr(string name);
	virtual int stop_Attr(string name);

private:
	template <typename Type> int store_scalar(string attr, vector<Type> value_r, vector<Type> value_w, int write_type/*READ, READ_WRITE, ..*/, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool isNull=false);
	template <typename Type> int store_array(string attr, vector<Type> value_r, vector<Type> value_w, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool isNull=false);
	int store_scalar_string(string attr, vector<string> value_r, vector<string> value_w, int write_type/*READ, READ_WRITE, ..*/, double ev_time, double rcv_time, string table_name, bool isNull=false);
	int store_array_string(string attr, vector<string> value_r, vector<string> value_w, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, bool isNull=false);
};

class HdbPPMySQLFactory : public DBFactory
{

public:
	virtual AbstractDB* create_db(string host, string user, string password, string dbname, int port);

};

#endif

