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
#include <libhdb++/LibHdb++.h>

#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <queue>
#include <stdint.h>

//Tango:
#include <tango.h>
//#include <event.h>

#define TYPE_SCALAR					"scalar"
#define TYPE_ARRAY					"array"

#define TYPE_DEV_BOOLEAN			"devboolean"
#define TYPE_DEV_UCHAR				"devuchar"
#define TYPE_DEV_SHORT				"devshort"
#define TYPE_DEV_USHORT				"devushort"
#define TYPE_DEV_LONG				"devlong"
#define TYPE_DEV_ULONG				"devulong"
#define TYPE_DEV_LONG64				"devlong64"
#define TYPE_DEV_ULONG64			"devulong64"
#define TYPE_DEV_FLOAT				"devfloat"
#define TYPE_DEV_DOUBLE				"devdouble"
#define TYPE_DEV_STRING				"devstring"
#define TYPE_DEV_STATE				"devstate"
#define TYPE_DEV_ENCODED			"devencoded"
#define TYPE_DEV_ENUM				"devenum"

#define TYPE_RO						"ro"
#define TYPE_RW						"rw"

#define EVENT_ADD					"add"
#define EVENT_REMOVE				"remove"
#define EVENT_START					"start"
#define EVENT_STOP					"stop"
#define EVENT_CRASH					"crash"
#define EVENT_PAUSE					"pause"

//######## att_conf ########
#define CONF_TABLE_NAME				"att_conf"
#define CONF_COL_ID					"att_conf_id"
#define CONF_COL_NAME				"att_name"
#define CONF_COL_TYPE_ID			"att_conf_data_type_id"
#define CONF_COL_TTL				"att_ttl"
#define CONF_COL_FACILITY			"facility"
#define CONF_COL_DOMAIN				"domain"
#define CONF_COL_FAMILY				"family"
#define CONF_COL_MEMBER				"member"
#define CONF_COL_LAST_NAME			"name"

//######## att_conf_data_type ########
#define CONF_TYPE_TABLE_NAME		"att_conf_data_type"
#define CONF_TYPE_COL_TYPE_ID		"att_conf_data_type_id"
#define CONF_TYPE_COL_TYPE			"data_type"

//######## att_history ########
#define HISTORY_TABLE_NAME			"att_history"
#define HISTORY_COL_ID				"att_conf_id"
#define HISTORY_COL_EVENT_ID		"att_history_event_id"
#define HISTORY_COL_TIME			"time"

//######## att_history_event ########
#define HISTORY_EVENT_TABLE_NAME	"att_history_event"
#define HISTORY_EVENT_COL_EVENT_ID	"att_history_event_id"
#define HISTORY_EVENT_COL_EVENT		"event"

//######## att_scalar_... ########
#define SC_COL_ID					"att_conf_id"
#define SC_COL_INS_TIME				"insert_time"
#define SC_COL_RCV_TIME				"recv_time"
#define SC_COL_EV_TIME				"data_time"
#define SC_COL_VALUE_R				"value_r"
#define SC_COL_VALUE_W				"value_w"
#define SC_COL_QUALITY				"quality"
#define SC_COL_ERROR_DESC_ID		"att_error_desc_id"



//######## att_array_... ########
#define ARR_COL_ID					SC_COL_ID
#define ARR_COL_INS_TIME			SC_COL_INS_TIME
#define ARR_COL_RCV_TIME			SC_COL_RCV_TIME
#define ARR_COL_EV_TIME				SC_COL_EV_TIME
#define ARR_COL_VALUE_R				SC_COL_VALUE_R
#define ARR_COL_VALUE_W				SC_COL_VALUE_W
#define ARR_COL_IDX					"idx"
#define ARR_COL_DIMX_R				"dim_x_r"
#define ARR_COL_DIMY_R				"dim_y_r"
#define ARR_COL_DIMX_W				"dim_x_w"
#define ARR_COL_DIMY_W				"dim_y_w"
#define ARR_COL_QUALITY				SC_COL_QUALITY
#define ARR_COL_ERROR_DESC_ID		SC_COL_ERROR_DESC_ID

//######## att_error_desc ########
#define ERR_TABLE_NAME				"att_error_desc"
#define ERR_COL_ID					"att_error_desc_id"
#define ERR_COL_ERROR_DESC			"error_desc"


//######## att_parameter ########
#define PARAM_TABLE_NAME				"att_parameter"
#define PARAM_COL_ID					"att_conf_id"
#define PARAM_COL_INS_TIME				"insert_time"
#define PARAM_COL_EV_TIME				"recv_time"
#define PARAM_COL_LABEL					"label"
#define PARAM_COL_UNIT					"unit"
#define PARAM_COL_STANDARDUNIT			"standard_unit"
#define PARAM_COL_DISPLAYUNIT			"display_unit"
#define PARAM_COL_FORMAT				"format"
#define PARAM_COL_ARCHIVERELCHANGE		"archive_rel_change"
#define PARAM_COL_ARCHIVEABSCHANGE		"archive_abs_change"
#define PARAM_COL_ARCHIVEPERIOD			"archive_period"
#define PARAM_COL_DESCRIPTION			"description"

//######## INFORMATION SCHEMA ########
#define INFORMATION_SCHEMA				"INFORMATION_SCHEMA"
#define INF_SCHEMA_COLUMN_NAME			"COLUMN_NAME"
#define INF_SCHEMA_COLUMNS				"COLUMNS"
#define INF_SCHEMA_TABLE_SCHEMA			"TABLE_SCHEMA"
#define INF_SCHEMA_TABLE_NAME			"TABLE_NAME"


class HdbPPMySQL : public AbstractDB
{
private:

	MYSQL *dbp;
	string m_dbname;
	map<string,int> attr_ID_map;
	bool lightschema;	//without recv_time and insert_time
	bool autodetectschema;
	map<string,int> attr_ERR_ID_map;
	queue<string> attr_ERR_queue;
	
	vector<Tango::CmdArgType> v_type;/*DEV_DOUBLE, DEV_STRING, ..*/
	vector<Tango::AttrDataFormat> v_format;/*SCALAR, SPECTRUM, ..*/
	vector<Tango::AttrWriteType> v_write_type;/*READ, READ_WRITE, ..*/
	map<string,bool > table_column_map;

	string get_only_attr_name(string str);
	string get_only_tango_host(string str);
#ifndef _MULTI_TANGO_HOST
	string remove_domain(string facility);
	string add_domain(string facility);
#endif
	void string_explode(string str, string separator, vector<string>* results);
	void string_vector2map(vector<string> str, string separator, map<string,string>* results);

	string get_data_type(int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/);
	string get_table_name(int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/);
	bool autodetect_column(string table_name, string column_name);

public:

	~HdbPPMySQL();

	HdbPPMySQL(vector<string> configuration);

	//void connect_db(string host, string user, string password, string dbname);
	int find_attr_id(string facility, string attr_name, int &ID);
	int find_attr_id_type(string facility, string attr_name, int &ID, string attr_type, unsigned int &conf_ttl);
	int find_last_event(int ID, string &event);
	int find_err_id(string err, int &ERR_ID);
	void cache_err_id(string error_desc, int &ERR_ID);
	int insert_error(string err, int &ERR_ID);
	virtual void insert_Attr(Tango::EventData *data, HdbEventDataType ev_data_type);
	virtual void insert_param_Attr(Tango::AttrConfEventData *data, HdbEventDataType ev_data_type);
	virtual void configure_Attr(string name, int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/, unsigned int ttl/*hours, 0=infinity*/);
	virtual void updateTTL_Attr(string name, unsigned int ttl/*hours, 0=infinity*/);
	virtual void event_Attr(string name, unsigned char event);

private:
	template <typename Type> void extract_and_store(string attr_name, Tango::EventData *data, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, Tango::AttrDataFormat data_format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool _is_unsigned, bool isNull);
	template <typename Type> void store_scalar(string attr, vector<Type> value_r, vector<Type> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool is_unsigned, bool isNull=false);
	template <typename Type> void store_array(string attr, vector<Type> value_r, vector<Type> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool _is_unsigned, bool isNull=false);
	void store_scalar_string(string attr, vector<string> value_r, vector<string> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, double ev_time, double rcv_time, string table_name, bool isNull=false);
	void store_array_string(string attr, vector<string> value_r, vector<string> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, bool isNull=false);
	template <typename Type> bool is_nan_or_inf(Type val);
};

class HdbPPMySQLFactory : public DBFactory
{

public:
	virtual AbstractDB* create_db(vector<string> configuration);

};

#endif

