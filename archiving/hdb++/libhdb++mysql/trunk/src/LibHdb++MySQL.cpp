//=============================================================================
//
// file :        LibHdb++MySQL.cpp
//
// description : Source file for the LibHdb++MySQL library.
//
// Author: Graziano Scalamera
//
// $Revision: 1.2 $
//
// $Log: LibHdb++MySQL.cpp,v $
// Revision 1.2  2014-03-07 14:50:46  graziano
// fixed domain
//
// Revision 1.1  2014-03-07 13:17:22  graziano
// first commit
//
//
//
//
//=============================================================================

#include "LibHdb++MySQL.h"
#include <stdlib.h>
#include <mysql.h>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <netdb.h> //for getaddrinfo


#ifndef LIB_BUILDTIME
#define LIB_BUILDTIME   RELEASE " " __DATE__ " "  __TIME__
#endif

const char version_string[] = "$Build: " LIB_BUILDTIME " $";
static const char __FILE__rev[] = __FILE__ " $Id: $";

//#define _LIB_DEBUG

HdbPPMySQL::HdbPPMySQL(string host, string user, string password, string dbname, int port)
{
	m_dbname = dbname;
	dbp = new MYSQL();
	if(!mysql_init(dbp))
	{
		cout << __func__<<": VERSION: " << version_string << " file:" << __FILE__rev << endl;
		cout << __func__<< ": mysql init db error: "<< mysql_error(dbp) << endl;
	}
	my_bool my_auto_reconnect=1;
	if(mysql_options(dbp,MYSQL_OPT_RECONNECT,&my_auto_reconnect) !=0)
	{
		cout << __func__<<": mysql auto reconnection error: " << mysql_error(dbp) << endl;
	}



	if(!mysql_real_connect(dbp, host.c_str(), user.c_str(), password.c_str(), dbname.c_str(), port, NULL, 0))
	{
		cout << __func__<< ": mysql connect db error: "<< mysql_error(dbp) << endl;
	}
	else
	{
		//everything OK
#ifdef _LIB_DEBUG
		cout << __func__<< ": mysql connection OK" << endl;
#endif
	}

}

HdbPPMySQL::~HdbPPMySQL()
{
	mysql_close(dbp);
	delete dbp;
}

int HdbPPMySQL::find_attr_id(string facility, string attr, int &ID)
{
	ostringstream query_str;
	//string facility_no_domain = remove_domain(facility);
	//string facility_with_domain = add_domain(facility);
#ifndef _MULTI_TANGO_HOST
	query_str << 
		"SELECT " << CONF_COL_ID << " FROM " << m_dbname << "." << CONF_TABLE_NAME <<
			" WHERE " << CONF_COL_NAME << " = 'tango://" << facility<<"/"<<attr << "'";
#else
	vector<string> facilities;
	string_explode(facility,",",&facilities);

	query_str << 
		"SELECT " << CONF_COL_ID << " FROM " << m_dbname << "." << CONF_TABLE_NAME <<
			" WHERE (";

	for(vector<string>::iterator it = facilities.begin(); it != facilities.end(); it++)
	{
		query_str << CONF_COL_NAME<< " LIKE 'tango://%"<<*it<<"%/"<<attr<< "'";
		if(it != facilities.end() - 1)
			query_str << " OR ";
	}
	query_str << ")";
#endif

	if(mysql_query(dbp, query_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
		return -1;
	}
	else
	{
		MYSQL_RES *res;
		MYSQL_ROW row;
		/*res = mysql_use_result(dbp);
		my_ulonglong num_found = mysql_num_rows(res);
		if(num_found == 0)*/
		res = mysql_store_result(dbp);
		if(res == NULL)
		{
			cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
			return -1;
		}
#ifdef _LIB_DEBUG
		else
		{
			my_ulonglong num_found = mysql_num_rows(res);
			if(num_found > 0)
			{
				cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
			}
			else
			{
				cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
				mysql_free_result(res);
				return -1;
			}
		}
#endif
		bool found = false;
		while ((row = mysql_fetch_row(res)))
		{
			found = true;
			ID = atoi(row[0]);
		}	
		mysql_free_result(res);
		if(!found)
			return -1;
	}
	return 0;
}

int HdbPPMySQL::find_attr_id_type(string facility, string attr, int &ID, string attr_type)
{
	ostringstream query_str;
//	string facility_no_domain = remove_domain(facility);
	string db_type;
#ifndef _MULTI_TANGO_HOST
	query_str << 
		"SELECT " << CONF_COL_ID << "," << CONF_COL_TYPE << " FROM " << m_dbname << "." << CONF_TABLE_NAME <<
			" WHERE " << CONF_COL_NAME << " = 'tango://" << facility<<"/"<<attr << "'";
#else
	vector<string> facilities;
	string_explode(facility,",",&facilities);

	query_str << 
		"SELECT " << CONF_COL_ID << "," << CONF_COL_TYPE << " FROM " << m_dbname << "." << CONF_TABLE_NAME <<
			" WHERE (";

	for(vector<string>::iterator it = facilities.begin(); it != facilities.end(); it++)
	{
		query_str << CONF_COL_NAME<< " LIKE 'tango://%"<<*it<<"%/"<<attr<< "'";
		if(it != facilities.end() - 1)
			query_str << " OR ";
	}
	query_str << ")";
#endif
	
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << " err="<<mysql_error(dbp)<< endl;
		return -1;
	}
	else
	{
		MYSQL_RES *res;
		MYSQL_ROW row;
		/*res = mysql_use_result(dbp);
		my_ulonglong num_found = mysql_num_rows(res);
		if(num_found == 0)*/
		res = mysql_store_result(dbp);
		if(res == NULL)
		{
			cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
			return -1;
		}
#ifdef _LIB_DEBUG
		else
		{
			my_ulonglong num_found = mysql_num_rows(res);
			if(num_found > 0)
			{
				cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
			}
			else
			{
				cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
				mysql_free_result(res);
				return -1;
			}
		}
#endif
		bool found = false;
		while ((row = mysql_fetch_row(res)))
		{
			found = true;
			ID = atoi(row[0]);
			db_type = row[1];
		}
		mysql_free_result(res);
		if(!found)
			return -1;

		if(db_type != attr_type)
		{
			cout << __func__<< ": FOUND ID="<<ID<<" but different type: attr_type="<<attr_type<<"-db_type="<<db_type << endl;
			return -2;
		}
		else
		{
			cout << __func__<< ": FOUND ID="<<ID<<" with SAME type: attr_type="<<attr_type<<"-db_type="<<db_type << endl;
			return 0;
		}
	}
	return 0;
}

int HdbPPMySQL::insert_Attr(Tango::EventData *data, HdbEventDataType ev_data_type)
{
	int ret = -1;
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	try
	{
		string attr_name = data->attr_name;
		double	ev_time;
		double	rcv_time = data->get_date().tv_sec + (double)data->get_date().tv_usec/1.0e6;
		int quality = (int)data->attr_value->get_quality();
		string error_desc("");
#ifdef _LIB_DEBUG
		cout << __func__<< ": entering quality="<<quality << endl;
#endif

		vector<double>	vdval_r;
		vector<double>	vdval_w;

		vector<int64_t>	vi64val_r;
		vector<int64_t>	vi64val_w;

		vector<int8_t>	vi8val_r;
		vector<int8_t>	vi8val_w;

		vector<string>	vsval_r;
		vector<string>	vsval_w;
#if 0
		Tango::AttributeDimension attr_w_dim = data->attr_value->get_w_dimension();
		Tango::AttributeDimension attr_r_dim = data->attr_value->get_r_dimension();
		int data_type = data->attr_value->get_type();
		//Tango::AttrDataFormat data_format = data->attr_value->get_data_format();	//Tango::AttrDataFormat //TODO: test if SCALAR, SPECTRUM, ...
		Tango::AttrDataFormat data_format = (attr_w_dim.dim_x <= 1 && attr_w_dim.dim_y <= 1 && attr_r_dim.dim_x <= 1 && attr_r_dim.dim_y <= 1) ?
					Tango::SCALAR : Tango::SPECTRUM;	//TODO
		int write_type = (attr_w_dim.dim_x == 0 && attr_w_dim.dim_y == 0) ? Tango::READ : Tango::READ_WRITE;	//TODO
#else
		Tango::AttributeDimension attr_w_dim;
		Tango::AttributeDimension attr_r_dim;
		int data_type = ev_data_type.data_type; //data->attr_value->get_type()
		Tango::AttrDataFormat data_format = ev_data_type.data_format;
		int write_type = ev_data_type.write_type;
		int max_dim_x = ev_data_type.max_dim_x;
		int max_dim_y = ev_data_type.max_dim_y;
#endif
		ev_time = data->attr_value->get_date().tv_sec + (double)data->attr_value->get_date().tv_usec/1.0e6;
		bool isNull = false;
		data->attr_value->reset_exceptions(Tango::DeviceAttribute::isempty_flag); //disable is_empty exception
		if(data->err || data->attr_value->is_empty()/* || data->attr_value->get_quality() == Tango::ATTR_INVALID */)
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": going to archive as NULL..." << endl;
#endif
			isNull = true;
			if(data->err)
			{
				error_desc = data->errors[0].desc;
				ev_time = rcv_time;
			}
		}
#ifdef _LIB_DEBUG
		cout << __func__<< ": data_type="<<data_type<<" data_format="<<data_format<<" write_type="<<write_type << endl;
#endif
		if(!isNull)
		{
			attr_w_dim = data->attr_value->get_w_dimension();
			attr_r_dim = data->attr_value->get_r_dimension();
		}
		else
		{
			attr_r_dim.dim_x = 1;//max_dim_x;//TODO: OK?
			attr_w_dim.dim_x = 0;//max_dim_x;//TODO: OK?
			attr_r_dim.dim_y = 0;//max_dim_y;//TODO: OK?
			attr_w_dim.dim_y = 0;//max_dim_y;//TODO: OK?
		}

		string table_name = get_table_name(data_type, data_format, write_type);

		switch(data_type)
		{
		case Tango::DEV_DOUBLE:
		{
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(vdval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(vdval_r) && data->attr_value->extract_set(vdval_w))))	//TODO: WO
			{
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_FLOAT:
		{
			vector<float>	fval_r;
			vector<float>	fval_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(fval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(fval_r) && data->attr_value->extract_set(fval_w))))
			{
				for (unsigned int i=0 ; i<fval_r.size() ; i++)
					vdval_r.push_back((double)fval_r[i]);
				for (unsigned int i=0 ; i<fval_w.size() ; i++)
					vdval_w.push_back((double)fval_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_LONG:
		{
			vector<Tango::DevLong>	lval_r;
			vector<Tango::DevLong>	lval_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(lval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(lval_r) && data->attr_value->extract_set(lval_w))))
			{
				for (unsigned int i=0 ; i<lval_r.size() ; i++)
					vi64val_r.push_back((int64_t)lval_r[i]);
				for (unsigned int i=0 ; i<lval_w.size() ; i++)
					vi64val_w.push_back((int64_t)lval_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
				else
					ret = store_array<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_ULONG:
		{
			vector<Tango::DevULong>	ulval_r;
			vector<Tango::DevULong>	ulval_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(ulval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(ulval_r) && data->attr_value->extract_set(ulval_w))))
			{
				for (unsigned int i=0 ; i<ulval_r.size() ; i++)
					vi64val_r.push_back((int64_t)ulval_r[i]);
				for (unsigned int i=0 ; i<ulval_w.size() ; i++)
					vi64val_w.push_back((int64_t)ulval_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
				else
					ret = store_array<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_LONG64:
		{
			vector<Tango::DevLong64>	l64val_r;
			vector<Tango::DevLong64>	l64val_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(l64val_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(l64val_r) && data->attr_value->extract_set(l64val_w))))
			{
				for (unsigned int i=0 ; i<l64val_r.size() ; i++)
					vi64val_r.push_back((int64_t)l64val_r[i]);
				for (unsigned int i=0 ; i<l64val_w.size() ; i++)
					vi64val_w.push_back((int64_t)l64val_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
				else
					ret = store_array<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_ULONG64:
		{
			vector<Tango::DevULong64>	ul64val_r;
			vector<Tango::DevULong64>	ul64val_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(ul64val_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(ul64val_r) && data->attr_value->extract_set(ul64val_w))))
			{
				for (unsigned int i=0 ; i<ul64val_r.size() ; i++)
					vi64val_r.push_back((int64_t)ul64val_r[i]);
				for (unsigned int i=0 ; i<ul64val_w.size() ; i++)
					vi64val_w.push_back((int64_t)ul64val_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
				else
					ret = store_array<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_SHORT:
		{
			vector<Tango::DevShort>	sval_r;
			vector<Tango::DevShort>	sval_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(sval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(sval_r) && data->attr_value->extract_set(sval_w))))
			{
				for (unsigned int i=0 ; i<sval_r.size() ; i++)
					vi64val_r.push_back((int64_t)sval_r[i]);
				for (unsigned int i=0 ; i<sval_w.size() ; i++)
					vi64val_w.push_back((int64_t)sval_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
				else
					ret = store_array<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_USHORT:
		{
			vector<Tango::DevUShort>	usval_r;
			vector<Tango::DevUShort>	usval_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(usval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(usval_r) && data->attr_value->extract_set(usval_w))))
			{
				for (unsigned int i=0 ; i<usval_r.size() ; i++)
					vi64val_r.push_back((int64_t)usval_r[i]);
				for (unsigned int i=0 ; i<usval_w.size() ; i++)
					vi64val_w.push_back((int64_t)usval_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
				else
					ret = store_array<int64_t>(attr_name, vi64val_r, vi64val_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_BOOLEAN:
		{
			vector<Tango::DevBoolean>	bval_r;
			vector<Tango::DevBoolean>	bval_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(bval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(bval_r) && data->attr_value->extract_set(bval_w))))
			{
				for (unsigned int i=0 ; i<bval_r.size() ; i++)
					vi8val_r.push_back((int8_t)bval_r[i]);
				for (unsigned int i=0 ; i<bval_w.size() ; i++)
					vi8val_w.push_back((int8_t)bval_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<int8_t>(attr_name, vi8val_r, vi8val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY);
				else
					ret = store_array<int8_t>(attr_name, vi8val_r, vi8val_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_UCHAR:
		{
			vector<Tango::DevUChar>	ucval_r;
			vector<Tango::DevUChar>	ucval_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(ucval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(ucval_r) && data->attr_value->extract_set(ucval_w))))
			{
				for (unsigned int i=0 ; i<ucval_r.size() ; i++)
					vi8val_r.push_back((int8_t)ucval_r[i]);
				for (unsigned int i=0 ; i<ucval_w.size() ; i++)
					vi8val_w.push_back((int8_t)ucval_w[i]);

				if(data_format == Tango::SCALAR)
					ret = store_scalar<int8_t>(attr_name, vi8val_r, vi8val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY);
				else
					ret = store_array<int8_t>(attr_name, vi8val_r, vi8val_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY);
			}
			else
			{
				vdval_r.push_back(0); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				else
					ret = store_array<double>(attr_name, vdval_r, vdval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_STRING:
		{
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(vsval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(vsval_r) && data->attr_value->extract_set(vsval_w))))	//TODO: WO
			{
				if(data_format == Tango::SCALAR)
					ret = store_scalar_string(attr_name, vsval_r, vsval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name);
				else
					ret = store_array_string(attr_name, vsval_r, vsval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name);
			}
			else
			{
				vsval_r.push_back(""); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar_string(attr_name, vsval_r, vsval_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, true);
				else
					ret = store_array_string(attr_name, vsval_r, vsval_w, quality, error_desc, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_STATE:
		{
			Tango::DevState	st;
			*data->attr_value >> st;
			vi8val_r.push_back((int8_t)st);
			ret = store_scalar<int8_t>(attr_name, vi8val_r, vi8val_w, quality, error_desc, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY);
			break;
		}
		default:
		{
			TangoSys_MemStream	os;
			os << "Attribute " << data->attr_name<< " type (" << (int)(data->attr_value->get_type()) << ") not supported";
			cout << __func__<<": " << os.str() << endl;
			return -1;
		}
		}
	}
	catch(Tango::DevFailed &e)
	{

		cout << "Exception on " << data->attr_name << ":" << endl;
		
		for (unsigned int i=0; i<e.errors.length(); i++)
		{
			cout << e.errors[i].reason << endl;
			cout << e.errors[i].desc << endl;
			cout << e.errors[i].origin << endl;
		}
 
		cout << endl;	
	}
#ifdef _LIB_DEBUG
//	cout << __func__<< ": exiting... ret="<<ret << endl;
#endif
	return ret;
}

int HdbPPMySQL::insert_param_Attr(Tango::AttrConfEventData *data, HdbEventDataType ev_data_type)
{
	int ret = -1;
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	try
	{
		string attr = data->attr_name;
		double	ev_time = data->get_date().tv_sec + (double)data->get_date().tv_usec/1.0e6;
		string error_desc("");


		map<string,int>::iterator it = attr_ID_map.find(attr);
		//if not already present in cache, look for ID in the DB
		if(it == attr_ID_map.end())
		{
			int ID=-1;
			string facility = get_only_tango_host(attr);
			string attr_name = get_only_attr_name(attr);
			find_attr_id(facility, attr_name, ID);
			if(ID != -1)
			{
				attr_ID_map.insert(make_pair(attr,ID));
				it = attr_ID_map.find(attr);
			}
			else
			{
				cout << __func__<< ": ID not found!" << endl;
				return -1;
			}
		}
		if(it == attr_ID_map.end())
		{
			cout << __func__<< ": ID not found for attr="<<attr << endl;
			return -1;
		}
		int ID=it->second;
		ostringstream query_str;

		query_str <<
			"INSERT INTO " << m_dbname << "." << PARAM_TABLE_NAME <<
				" (" << PARAM_COL_ID << "," << PARAM_COL_INS_TIME << "," << PARAM_COL_EV_TIME << "," <<
				PARAM_COL_LABEL << "," << PARAM_COL_UNIT << "," << PARAM_COL_STANDARDUNIT << "," <<
				PARAM_COL_DISPLAYUNIT << "," << PARAM_COL_FORMAT << "," << PARAM_COL_ARCHIVERELCHANGE << "," <<
				PARAM_COL_ARCHIVEABSCHANGE << "," << PARAM_COL_ARCHIVEPERIOD << ")";

		query_str << " VALUES (?,NOW(6),FROM_UNIXTIME(?)," <<
				"?,?,?," <<
				"?,?,?," <<
				"?,?)" ;

		MYSQL_STMT	*pstmt;
		MYSQL_BIND	plog_bind[10];
		double		double_data;
		int			int_data;
		string		param_data[8];
		unsigned long param_data_len[8];
		pstmt = mysql_stmt_init(dbp);
		if (!pstmt)
		{
			cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
			return -1;
		}
		if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
		{
			cout << __func__<< ": mysql_stmt_prepare(), INSERT failed query=" << query_str.str() << endl;
			return -1;
		}

		int_data = ID;
		double_data = ev_time;
		param_data[0] = data->attr_conf->label;
		param_data_len[0] = param_data[0].length();
		param_data[1] = data->attr_conf->unit;
		param_data_len[1] = param_data[1].length();
		param_data[2] = data->attr_conf->standard_unit;
		param_data_len[2] = param_data[2].length();
		param_data[3] = data->attr_conf->display_unit;
		param_data_len[3] = param_data[3].length();
		param_data[4] = data->attr_conf->format;
		param_data_len[4] = param_data[4].length();
		param_data[5] = data->attr_conf->events.arch_event.archive_rel_change;
		param_data_len[5] = param_data[5].length();
		param_data[6] = data->attr_conf->events.arch_event.archive_abs_change;
		param_data_len[6] = param_data[6].length();
		param_data[7] = data->attr_conf->events.arch_event.archive_period;
		param_data_len[7] = param_data[7].length();

		memset(plog_bind, 0, sizeof(plog_bind));

		plog_bind[0].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[0].buffer= (void *)&int_data;
		plog_bind[0].is_null= 0;
		plog_bind[0].length= 0;

		plog_bind[1].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[1].buffer= (void *)&double_data;
		plog_bind[1].is_null= 0;
		plog_bind[1].length= 0;

		plog_bind[2].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[2].buffer= (void *)param_data[0].c_str();	//TODO: escape
		plog_bind[2].is_null= 0;
		plog_bind[2].length= &param_data_len[0];

		plog_bind[3].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[3].buffer= (void *)param_data[1].c_str();	//TODO: escape
		plog_bind[3].is_null= 0;
		plog_bind[3].length= &param_data_len[1];

		plog_bind[4].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[4].buffer= (void *)param_data[2].c_str();	//TODO: escape
		plog_bind[4].is_null= 0;
		plog_bind[4].length= &param_data_len[2];

		plog_bind[5].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[5].buffer= (void *)param_data[3].c_str();	//TODO: escape
		plog_bind[5].is_null= 0;
		plog_bind[5].length= &param_data_len[3];

		plog_bind[6].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[6].buffer= (void *)param_data[4].c_str();	//TODO: escape
		plog_bind[6].is_null= 0;
		plog_bind[6].length= &param_data_len[4];

		plog_bind[7].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[7].buffer= (void *)param_data[5].c_str();	//TODO: escape
		plog_bind[7].is_null= 0;
		plog_bind[7].length= &param_data_len[5];

		plog_bind[8].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[8].buffer= (void *)param_data[6].c_str();	//TODO: escape
		plog_bind[8].is_null= 0;
		plog_bind[8].length= &param_data_len[6];

		plog_bind[9].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[9].buffer= (void *)param_data[7].c_str();	//TODO: escape
		plog_bind[9].is_null= 0;
		plog_bind[9].length= &param_data_len[7];

		if (mysql_stmt_bind_param(pstmt, plog_bind))
		{
			cout << __func__<< ": mysql_stmt_bind_param() failed" << endl;
			return -1;
		}

		if (mysql_stmt_execute(pstmt))
		{
			cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
			if (mysql_stmt_close(pstmt))
				cout << __func__<< ": failed while closing the statement" << endl;
			return -1;
		}
#ifdef _LIB_DEBUG
		else
		{
			cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
		}
#endif

	/*		if (paffected_rows != 1)
				DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
		if (mysql_stmt_close(pstmt))
		{
			cout << __func__<< ": failed while closing the statement" << endl;
			return -1;
		}
	}
	catch(Tango::DevFailed &e)
	{

		cout << "Exception on " << data->attr_name << ":" << endl;

		for (unsigned int i=0; i<e.errors.length(); i++)
		{
			cout << e.errors[i].reason << endl;
			cout << e.errors[i].desc << endl;
			cout << e.errors[i].origin << endl;
		}

		cout << endl;
	}
#ifdef _LIB_DEBUG
//	cout << __func__<< ": exiting... ret="<<ret << endl;
#endif
	return ret;
}

int HdbPPMySQL::configure_Attr(string name, int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/)
{
	ostringstream insert_str;
	ostringstream insert_event_str;
	string facility = get_only_tango_host(name);
#ifndef _MULTI_TANGO_HOST
	facility = add_domain(facility);
#endif
	string attr_name = get_only_attr_name(name);
	cout<< __func__ << ": name="<<name<<" -> facility="<<facility<<" attr_name="<<attr_name<< endl;
	int id=-1;
	string data_type = get_data_type(type, format, write_type);
	int ret = find_attr_id_type(facility, attr_name, id, data_type);
	//ID already present but different configuration (attribute type)
	if(ret == -2)
	{
		cout<< __func__ << ": ERROR "<<facility<<"/"<<attr_name<<" already configured with ID="<<id << endl;
		return -1;
	}

	//ID found and same configuration (attribute type): do nothing
	if(ret == 0)
	{
		cout<< __func__ << ": ALREADY CONFIGURED with same configuration: "<<facility<<"/"<<attr_name<<" with ID="<<id << endl;
		return 0;
	}

	//add domain name to fqdn
	name = string("tango://")+facility+string("/")+attr_name;
	insert_str <<
		"INSERT INTO " << m_dbname << "." << CONF_TABLE_NAME << " ("<<CONF_COL_NAME<<","<<CONF_COL_TYPE<<")" <<
			" VALUES ('" << name << "','" << data_type << "')";

	if(mysql_query(dbp, insert_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << insert_str.str() << endl;
		return -1;
	}

	//int last_id = mysql_insert_id(dbp);

	insert_event_str <<
		"INSERT INTO " << m_dbname << "." << HISTORY_TABLE_NAME << " ("<<HISTORY_COL_ID<<","<<HISTORY_COL_EVENT<<","<<HISTORY_COL_TIME<<")" <<
			" VALUES (LAST_INSERT_ID(),'" << EVENT_ADD << "',NOW(6))";

	if(mysql_query(dbp, insert_event_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << insert_event_str.str() << endl;
		return -1;
	}

	return 0;
}

int HdbPPMySQL::remove_Attr(string name)
{
	//TODO: implement
	return 0;
}

int HdbPPMySQL::start_Attr(string name)
{
	ostringstream insert_event_str;
	string facility = get_only_tango_host(name);
#ifndef _MULTI_TANGO_HOST
	facility = add_domain(facility);
#endif
	string attr_name = get_only_attr_name(name);

	int id=0;
	int ret = find_attr_id(facility, attr_name, id);
	if(ret < 0)
	{
		cout<< __func__ << ": ERROR "<<facility<<"/"<<attr_name<<" NOT FOUND" << endl;
		return -1;
	}

	insert_event_str <<
		"INSERT INTO " << m_dbname << "." << HISTORY_TABLE_NAME << " ("<<HISTORY_COL_ID<<","<<HISTORY_COL_EVENT<<","<<HISTORY_COL_TIME<<")" <<
			" VALUES ("<<id<<",'" << EVENT_START << "',NOW(6))";

	if(mysql_query(dbp, insert_event_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << insert_event_str.str() << endl;
		return -1;
	}

	return 0;
}

int HdbPPMySQL::stop_Attr(string name)
{
	ostringstream insert_event_str;
	string facility = get_only_tango_host(name);
#ifndef _MULTI_TANGO_HOST
	facility = add_domain(facility);
#endif
	string attr_name = get_only_attr_name(name);

	int id=0;
	int ret = find_attr_id(facility, attr_name, id);
	if(ret < 0)
	{
		cout<< __func__ << ": ERROR "<<facility<<"/"<<attr_name<<" NOT FOUND" << endl;
		return -1;
	}

	insert_event_str <<
		"INSERT INTO " << m_dbname << "." << HISTORY_TABLE_NAME << " ("<<HISTORY_COL_ID<<","<<HISTORY_COL_EVENT<<","<<HISTORY_COL_TIME<<")" <<
			" VALUES ("<<id<<",'" << EVENT_STOP << "',NOW(6))";

	if(mysql_query(dbp, insert_event_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << insert_event_str.str() << endl;
		return -1;
	}

	return 0;
}

//=============================================================================
//=============================================================================
template <typename Type> int HdbPPMySQL::store_scalar(string attr, vector<Type> value_r, vector<Type> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int write_type/*READ, READ_WRITE, ..*/, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool isNull)
{
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	map<string,int>::iterator it = attr_ID_map.find(attr);
	//if not already present in cache, look for ID in the DB
	if(it == attr_ID_map.end())
	{
		int ID=-1;
		string facility = get_only_tango_host(attr);
		string attr_name = get_only_attr_name(attr);
		find_attr_id(facility, attr_name, ID);
		if(ID != -1)
		{
			attr_ID_map.insert(make_pair(attr,ID));
			it = attr_ID_map.find(attr);
		}
		else
		{
			cout << __func__<< ": ID not found!" << endl;
			return -1;
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found for attr="<<attr << endl;
		return -1;
	}
	int ID=it->second;
	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << table_name <<
			" (" << SC_COL_ID << "," << SC_COL_INS_TIME << "," << SC_COL_RCV_TIME << "," <<
			SC_COL_EV_TIME << "," << SC_COL_QUALITY << "," << SC_COL_ERROR_DESC << "," << SC_COL_VALUE_R;

	if(!(write_type == Tango::READ))	//RW
		query_str << "," << SC_COL_VALUE_W;

	query_str << ")";

	query_str << " VALUES (?,NOW(6),FROM_UNIXTIME(?),"
			<< "FROM_UNIXTIME(?),?,?,?";

	if(!(write_type == Tango::READ))	//RW
		query_str << ",?";

	query_str << ")";

	MYSQL_STMT	*pstmt;
	MYSQL_BIND	plog_bind[7];
	my_bool		is_null[2];    /* value nullability */
	double		double_data[2];
	Type		value_data[2];
	int			int_data[2];
	string		error_data;
	unsigned long error_data_len;
	my_bool		error_data_is_null;
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		return -1;
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		cout << __func__<< ": mysql_stmt_prepare(), INSERT failed" << endl;
		return -1;
	}

	if(value_r.size() >= 1 && !isNull)
	{
		if(std::isnan(value_r[0]) || std::isinf(value_r[0]))
		{
			is_null[0]=1;
			value_data[0]=0;	//useless
		}
		else
		{
			is_null[0]=0;
			value_data[0] = value_r[0];
		}
	}
	else
	{
		is_null[0]=1;
		value_data[0]=0;	//useless
	}

	if(value_w.size() >= 1 && !isNull)
	{
		if(std::isnan(value_w[0]) || std::isinf(value_w[0]))
		{
			is_null[1]=1;
			value_data[1]=0;	//useless
		}
		else
		{
			is_null[1]=0;
			value_data[1] = value_w[0];
		}
	}
	else
	{
		is_null[1]=1;
		value_data[1]=0;	//useless
	}

	int_data[0] = ID;
	int_data[1] = quality;
	double_data[0] = rcv_time;
	double_data[1] = ev_time;
	error_data = error_desc;
	error_data_len = error_data.length();
	if(error_data_len == 0)
		error_data_is_null = 1;
	else
		error_data_is_null = 0;
	memset(plog_bind, 0, sizeof(plog_bind));

	plog_bind[0].buffer_type= MYSQL_TYPE_LONG;
	plog_bind[0].buffer= (void *)&int_data[0];
	plog_bind[0].is_null= 0;
	plog_bind[0].length= 0;

	plog_bind[1].buffer_type= MYSQL_TYPE_DOUBLE;
	plog_bind[1].buffer= (void *)&double_data[0];
	plog_bind[1].is_null= 0;
	plog_bind[1].length= 0;

	plog_bind[2].buffer_type= MYSQL_TYPE_DOUBLE;
	plog_bind[2].buffer= (void *)&double_data[1];
	plog_bind[2].is_null= 0;
	plog_bind[2].length= 0;

	plog_bind[3].buffer_type= MYSQL_TYPE_LONG;
	plog_bind[3].buffer= (void *)&int_data[1];
	plog_bind[3].is_null= 0;
	plog_bind[3].length= 0;

	plog_bind[4].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[4].buffer= (void *)error_data.c_str();	//TODO: escape
	plog_bind[4].is_null= &error_data_is_null;
	error_data_len=error_data.length();
	plog_bind[4].length= &error_data_len;

	plog_bind[5].buffer_type= mysql_value_type;
	plog_bind[5].buffer= (void *)&value_data[0];
	plog_bind[5].is_null= &is_null[0];
	plog_bind[5].length= 0;

	plog_bind[6].buffer_type= mysql_value_type;
	plog_bind[6].buffer= (void *)&value_data[1];
	plog_bind[6].is_null= &is_null[1];
	plog_bind[6].length= 0;

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		cout << __func__<< ": mysql_stmt_bind_param() failed" << endl;
		return -1;
	}

	if (mysql_stmt_execute(pstmt))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << endl;
		return -1;
	}
#ifdef _LIB_DEBUG
	else
	{
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
	}
#endif

/*		if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
	if (mysql_stmt_close(pstmt))
	{
		cout << __func__<< ": failed while closing the statement" << endl;
		return -1;
	}

	return 0;
}

//=============================================================================
//=============================================================================
template <typename Type> int HdbPPMySQL::store_array(string attr, vector<Type> value_r, vector<Type> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool isNull)
{
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	map<string,int>::iterator it = attr_ID_map.find(attr);
	//if not already present in cache, look for ID in the DB
	if(it == attr_ID_map.end())
	{
		int ID=-1;
		string facility = get_only_tango_host(attr);
		string attr_name = get_only_attr_name(attr);
		find_attr_id(facility, attr_name, ID);
		if(ID != -1)
		{
			attr_ID_map.insert(make_pair(attr,ID));
			it = attr_ID_map.find(attr);
		}
		else
		{
			cout << __func__<< ": ID not found!" << endl;
			return -1;
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found fro attr="<<attr << endl;
		return -1;
	}
	int ID=it->second;
	uint32_t max_size = (value_r.size() > value_w.size()) ? value_r.size() : value_w.size();
	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << table_name <<
			" (" << ARR_COL_ID << "," << ARR_COL_INS_TIME << "," << ARR_COL_RCV_TIME << "," <<
			ARR_COL_EV_TIME << "," << ARR_COL_QUALITY << "," << ARR_COL_ERROR_DESC << "," << ARR_COL_IDX << "," << ARR_COL_DIMX << "," <<
			ARR_COL_DIMY << "," << ARR_COL_VALUE_R;

	if(!(write_type == Tango::READ))	//RW
		query_str << "," << ARR_COL_VALUE_W;

	query_str << ")";

	query_str << " VALUES ";

	for(int idx=0; idx < max_size; idx++)
	{
		query_str << "(?,NOW(6),FROM_UNIXTIME(?),"
			<< "FROM_UNIXTIME(?),?,?,?,?,?,?";

		if(!(write_type == Tango::READ))	//RW
			query_str << ",?";

		query_str << ")";
		if(idx < max_size-1)
			query_str << ",";
	}
	if(max_size == 0)
	{
		query_str << "(?,NOW(6),FROM_UNIXTIME(?),"
			<< "FROM_UNIXTIME(?),?,?,?,?";

		if(!(write_type == Tango::READ))	//RW
			query_str << ",?";

		query_str << ")";
	}
	int param_count_single = (write_type == Tango::READ) ? 9 : 10;	//param in single value insert
	int param_count = param_count_single*max_size;								//total param
	MYSQL_STMT	*pstmt;
	MYSQL_BIND	*plog_bind = new MYSQL_BIND[param_count];
	my_bool		is_null[2*max_size];    /* value nullability */	//value_r, value_w
	double		double_data[2*max_size];	// rcv_time, ev_time
	Type		value_data[2*max_size];		//value_r, value_w
	int			int_data[5*max_size];		//id, quality, idx, dimx, dimy
	string		error_data[max_size];
	unsigned long error_data_len[max_size];
	my_bool		error_data_is_null[max_size];
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		return -1;
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		cout << __func__<< ": mysql_stmt_prepare(), prepare stmt failed, stmt='"<<query_str.str()<<"' err="<<mysql_stmt_error(pstmt) << endl;
		return -1;
	}
	memset(plog_bind, 0, sizeof(MYSQL_BIND)*param_count);

	for(int idx=0; idx < max_size; idx++)
	{
		if(idx < value_r.size() && !isNull)
		{
			if(std::isnan(value_r[idx]) || std::isinf(value_r[idx]))
			{
				is_null[2*idx+0]=1;
				value_data[2*idx+0]=0;	//useless
			}
			else
			{
				is_null[2*idx+0]=0;
				value_data[2*idx+0] = value_r[idx];
			}
		}
		else
		{
			is_null[2*idx+0]=1;
			value_data[2*idx+0]=0;	//useless
		}

		if(idx < value_w.size() && !isNull)
		{
			if(std::isnan(value_w[idx]) || std::isinf(value_w[idx]))
			{
				is_null[2*idx+1]=1;
				value_data[2*idx+1]=0;	//useless
			}
			else
			{
				is_null[2*idx+1]=0;
				value_data[2*idx+1] = value_w[idx];
			}
		}
		else
		{
			is_null[2*idx+1]=1;
			value_data[2*idx+1]=0;	//useless
		}

		int_data[5*idx+0] = ID;
		int_data[5*idx+1] = quality;
		error_data[idx] = error_desc;
		error_data_len[idx] = error_data[idx].length();
		if(error_data_len[idx] == 0)
			error_data_is_null[idx] = 1;
		else
			error_data_is_null[idx] = 0;
		int_data[5*idx+2] = idx;
		int_data[5*idx+3] = attr_r_dim.dim_x;	//TODO: missing w sizes
		int_data[5*idx+4] = attr_r_dim.dim_y;	//TODO: missing w sizes
		double_data[2*idx+0] = rcv_time;
		double_data[2*idx+1] = ev_time;


		plog_bind[param_count_single*idx+0].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+0].buffer= (void *)&int_data[5*idx+0];
		plog_bind[param_count_single*idx+0].is_null= 0;
		plog_bind[param_count_single*idx+0].length= 0;

		plog_bind[param_count_single*idx+1].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+1].buffer= (void *)&double_data[2*idx+0];
		plog_bind[param_count_single*idx+1].is_null= 0;
		plog_bind[param_count_single*idx+1].length= 0;

		plog_bind[param_count_single*idx+2].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+2].buffer= (void *)&double_data[2*idx+1];
		plog_bind[param_count_single*idx+2].is_null= 0;
		plog_bind[param_count_single*idx+2].length= 0;

		plog_bind[param_count_single*idx+3].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+3].buffer= (void *)&int_data[5*idx+1];
		plog_bind[param_count_single*idx+3].is_null= 0;
		plog_bind[param_count_single*idx+3].length= 0;

		plog_bind[param_count_single*idx+4].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[param_count_single*idx+4].buffer= (void *)error_data[idx].c_str();	//TODO: escape
		plog_bind[param_count_single*idx+4].is_null= &error_data_is_null[idx];
		error_data_len[idx]=error_data[idx].length();
		plog_bind[param_count_single*idx+4].length= &error_data_len[idx];

		plog_bind[param_count_single*idx+5].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+5].buffer= (void *)&int_data[5*idx+2];
		plog_bind[param_count_single*idx+5].is_null= 0;
		plog_bind[param_count_single*idx+5].length= 0;

		plog_bind[param_count_single*idx+6].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+6].buffer= (void *)&int_data[5*idx+3];
		plog_bind[param_count_single*idx+6].is_null= 0;
		plog_bind[param_count_single*idx+6].length= 0;

		plog_bind[param_count_single*idx+7].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+7].buffer= (void *)&int_data[5*idx+4];
		plog_bind[param_count_single*idx+7].is_null= 0;
		plog_bind[param_count_single*idx+7].length= 0;

		plog_bind[param_count_single*idx+8].buffer_type= mysql_value_type;
		plog_bind[param_count_single*idx+8].buffer= (void *)&value_data[2*idx+0];
		plog_bind[param_count_single*idx+8].is_null= &is_null[2*idx+0];
		plog_bind[param_count_single*idx+8].length= 0;

		if(!(write_type == Tango::READ))
		{
			plog_bind[param_count_single*idx+9].buffer_type= mysql_value_type;
			plog_bind[param_count_single*idx+9].buffer= (void *)&value_data[2*idx+1];
			plog_bind[param_count_single*idx+9].is_null= &is_null[2*idx+1];
			plog_bind[param_count_single*idx+9].length= 0;
		}
	}

	if(max_size == 0)
	{
		int idx = 0;

		is_null[2*idx+0]=1;
		value_data[2*idx+0]=0;	//useless
		is_null[2*idx+1]=1;
		value_data[2*idx+1]=0;	//useless

		int_data[5*idx+0] = ID;
		int_data[5*idx+1] = quality;
		int_data[5*idx+2] = idx;
		int_data[5*idx+3] = attr_r_dim.dim_x;	//TODO: missing w sizes
		int_data[5*idx+4] = attr_r_dim.dim_y;	//TODO: missing w sizes
		double_data[2*idx+0] = rcv_time;
		double_data[2*idx+1] = ev_time;


		plog_bind[param_count_single*idx+0].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+0].buffer= (void *)&int_data[5*idx+0];
		plog_bind[param_count_single*idx+0].is_null= 0;
		plog_bind[param_count_single*idx+0].length= 0;

		plog_bind[param_count_single*idx+1].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+1].buffer= (void *)&double_data[2*idx+0];
		plog_bind[param_count_single*idx+1].is_null= 0;
		plog_bind[param_count_single*idx+1].length= 0;

		plog_bind[param_count_single*idx+2].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+2].buffer= (void *)&double_data[2*idx+1];
		plog_bind[param_count_single*idx+2].is_null= 0;
		plog_bind[param_count_single*idx+2].length= 0;

		plog_bind[param_count_single*idx+3].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+3].buffer= (void *)&int_data[5*idx+1];
		plog_bind[param_count_single*idx+3].is_null= 0;
		plog_bind[param_count_single*idx+3].length= 0;


		plog_bind[param_count_single*idx+4].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[param_count_single*idx+4].buffer= (void *)error_data[idx].c_str();	//TODO: escape
		plog_bind[param_count_single*idx+4].is_null= &error_data_is_null[idx];
		error_data_len[idx]=error_data[idx].length();
		plog_bind[param_count_single*idx+4].length= &error_data_len[idx];

		plog_bind[param_count_single*idx+5].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+5].buffer= (void *)&int_data[5*idx+2];
		plog_bind[param_count_single*idx+5].is_null= 0;
		plog_bind[param_count_single*idx+5].length= 0;

		plog_bind[param_count_single*idx+6].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+6].buffer= (void *)&int_data[5*idx+3];
		plog_bind[param_count_single*idx+6].is_null= 0;
		plog_bind[param_count_single*idx+6].length= 0;

		plog_bind[param_count_single*idx+7].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+7].buffer= (void *)&int_data[5*idx+4];
		plog_bind[param_count_single*idx+7].is_null= 0;
		plog_bind[param_count_single*idx+7].length= 0;

		plog_bind[param_count_single*idx+8].buffer_type= mysql_value_type;
		plog_bind[param_count_single*idx+8].buffer= (void *)&value_data[2*idx+0];
		plog_bind[param_count_single*idx+8].is_null= &is_null[2*idx+0];
		plog_bind[param_count_single*idx+8].length= 0;

		if(!(write_type == Tango::READ))
		{
			plog_bind[param_count_single*idx+9].buffer_type= mysql_value_type;
			plog_bind[param_count_single*idx+9].buffer= (void *)&value_data[2*idx+1];
			plog_bind[param_count_single*idx+9].is_null= &is_null[2*idx+1];
			plog_bind[param_count_single*idx+9].length= 0;
		}
	}

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		cout << __func__<< ": mysql_stmt_bind_param() failed" << endl;
		return -1;
	}

	if (mysql_stmt_execute(pstmt))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << endl;
		delete [] plog_bind;
		return -1;
	}
#ifdef _LIB_DEBUG
	else
	{
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
	}
#endif

/*		if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
	if (mysql_stmt_close(pstmt))
	{
		cout << __func__<< ": failed while closing the statement" << endl;
		delete [] plog_bind;
		return -1;
	}
	delete [] plog_bind;
	return 0;
}

//=============================================================================
//=============================================================================
int HdbPPMySQL::store_scalar_string(string attr, vector<string> value_r, vector<string> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int write_type/*READ, READ_WRITE, ..*/, double ev_time, double rcv_time, string table_name, bool isNull)
{
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	map<string,int>::iterator it = attr_ID_map.find(attr);
	//if not already present in cache, look for ID in the DB
	if(it == attr_ID_map.end())
	{
		int ID=-1;
		string facility = get_only_tango_host(attr);
		string attr_name = get_only_attr_name(attr);
		find_attr_id(facility, attr_name, ID);
		if(ID != -1)
		{
			attr_ID_map.insert(make_pair(attr,ID));
			it = attr_ID_map.find(attr);
		}
		else
		{
			cout << __func__<< ": ID not found!" << endl;
			return -1;
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found for attr="<<attr << endl;
		return -1;
	}
	int ID=it->second;
	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << table_name <<
			" (" << SC_COL_ID << "," << SC_COL_INS_TIME << "," << SC_COL_RCV_TIME << "," <<
			SC_COL_EV_TIME << "," << SC_COL_QUALITY << "," << SC_COL_ERROR_DESC << "," <<SC_COL_VALUE_R;

	if(!(write_type == Tango::READ))	//RW
		query_str << "," << SC_COL_VALUE_W;

	query_str << ")";

	query_str << " VALUES (?,NOW(6),FROM_UNIXTIME(?),"
			<< "FROM_UNIXTIME(?),?,?,?";

	if(!(write_type == Tango::READ))	//RW
		query_str << ",?";

	query_str << ")";

	MYSQL_STMT	*pstmt;
	MYSQL_BIND	plog_bind[7];
	my_bool		is_null[2];    /* value nullability */
	double		double_data[2];
	string		value_data[2];
	unsigned long value_data_len[2];
	int			int_data[2];
	string		error_data;
	unsigned long error_data_len;
	my_bool		error_data_is_null;
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		return -1;
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		cout << __func__<< ": mysql_stmt_prepare(), INSERT failed" << endl;
		return -1;
	}

	if(value_r.size() >= 1 && !isNull)
	{
		is_null[0]=0;
		value_data[0] = value_r[0];
#ifdef _LIB_DEBUG
		cout << __func__<< ": value not null to insert="<< value_data[0] << endl;
#endif
	}
	else
	{
		is_null[0]=1;
		value_data[0]="";	//useless
	}

	if(value_w.size() >= 1 && !isNull)
	{
			is_null[1]=0;
			value_data[1] = value_w[0];
	}
	else
	{
		is_null[1]=1;
		value_data[1]="";	//useless
	}

	int_data[0] = ID;
	int_data[1] = quality;
	double_data[0] = rcv_time;
	double_data[1] = ev_time;
	error_data = error_desc;
	error_data_len = error_data.length();
	if(error_data_len == 0)
		error_data_is_null = 1;
	else
		error_data_is_null = 0;
	memset(plog_bind, 0, sizeof(plog_bind));

	plog_bind[0].buffer_type= MYSQL_TYPE_LONG;
	plog_bind[0].buffer= (void *)&int_data[0];
	plog_bind[0].is_null= 0;
	plog_bind[0].length= 0;

	plog_bind[1].buffer_type= MYSQL_TYPE_DOUBLE;
	plog_bind[1].buffer= (void *)&double_data[0];
	plog_bind[1].is_null= 0;
	plog_bind[1].length= 0;

	plog_bind[2].buffer_type= MYSQL_TYPE_DOUBLE;
	plog_bind[2].buffer= (void *)&double_data[1];
	plog_bind[2].is_null= 0;
	plog_bind[2].length= 0;

	plog_bind[3].buffer_type= MYSQL_TYPE_LONG;
	plog_bind[3].buffer= (void *)&int_data[1];
	plog_bind[3].is_null= 0;
	plog_bind[3].length= 0;

	plog_bind[4].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[4].buffer= (void *)error_data.c_str();	//TODO: escape
	plog_bind[4].is_null= &error_data_is_null;
	error_data_len=error_data.length();
	plog_bind[4].length= &error_data_len;

	plog_bind[5].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[5].buffer= (void *)value_data[0].c_str();	//TODO: escape
	plog_bind[5].is_null= &is_null[0];
	value_data_len[0]=value_data[0].length();
	plog_bind[5].length= &value_data_len[0];

	plog_bind[6].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[6].buffer= (void *)value_data[1].c_str();	//TODO: escape
	plog_bind[6].is_null= &is_null[1];
	value_data_len[1]=value_data[1].length();
	plog_bind[6].length= &value_data_len[1];

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		cout << __func__<< ": mysql_stmt_bind_param() failed" << endl;
		return -1;
	}

	if (mysql_stmt_execute(pstmt))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << endl;
		return -1;
	}
#ifdef _LIB_DEBUG
	else
	{
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
	}
#endif

/*		if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
	if (mysql_stmt_close(pstmt))
	{
		cout << __func__<< ": failed while closing the statement" << endl;
		return -1;
	}

	return 0;
}

//=============================================================================
//=============================================================================
int HdbPPMySQL::store_array_string(string attr, vector<string> value_r, vector<string> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, bool isNull)
{
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	map<string,int>::iterator it = attr_ID_map.find(attr);
	//if not already present in cache, look for ID in the DB
	if(it == attr_ID_map.end())
	{
		int ID=-1;
		string facility = get_only_tango_host(attr);
		string attr_name = get_only_attr_name(attr);
		find_attr_id(facility, attr_name, ID);
		if(ID != -1)
		{
			attr_ID_map.insert(make_pair(attr,ID));
			it = attr_ID_map.find(attr);
		}
		else
		{
			cout << __func__<< ": ID not found!" << endl;
			return -1;
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found fro attr="<<attr << endl;
		return -1;
	}
	int ID=it->second;
	uint32_t max_size = (value_r.size() > value_w.size()) ? value_r.size() : value_w.size();
	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << table_name <<
			" (" << ARR_COL_ID << "," << ARR_COL_INS_TIME << "," << ARR_COL_RCV_TIME << "," <<
			ARR_COL_EV_TIME << "," << ARR_COL_QUALITY << "," << ARR_COL_ERROR_DESC << "," << ARR_COL_IDX << "," << ARR_COL_DIMX << "," <<
			ARR_COL_DIMY << "," << ARR_COL_VALUE_R;

	if(!(write_type == Tango::READ))	//RW
		query_str << "," << ARR_COL_VALUE_W;

	query_str << ")";

	query_str << " VALUES ";

	for(unsigned int idx=0; idx < max_size; idx++)
	{
		query_str << "(?,NOW(6),FROM_UNIXTIME(?),"
			<< "FROM_UNIXTIME(?),?,?,?,?,?,?";

		if(!(write_type == Tango::READ))	//RW
			query_str << ",?";

		query_str << ")";
		if(idx < max_size-1)
			query_str << ",";
	}
	int param_count_single = (write_type == Tango::READ) ? 9 : 10;	//param in single value insert
	int param_count = param_count_single*max_size;								//total param
	MYSQL_STMT	*pstmt;
	MYSQL_BIND	*plog_bind = new MYSQL_BIND[param_count];
	my_bool		is_null[2*max_size];    /* value nullability */	//value_r, value_w
	double		double_data[2*max_size];	// rcv_time, ev_time
	string		value_data[2*max_size];		//value_r, value_w
	unsigned long value_data_len[2*max_size];
	int			int_data[5*max_size];		//id, quality, idx, dimx, dimy
	string		error_data[max_size];
	unsigned long error_data_len[max_size];
	my_bool		error_data_is_null[max_size];
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		return -1;
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		cout << __func__<< ": mysql_stmt_prepare(), prepare stmt failed, stmt='"<<query_str.str()<<"' err="<<mysql_stmt_error(pstmt) << endl;
		return -1;
	}
	memset(plog_bind, 0, sizeof(MYSQL_BIND)*param_count);

	for(int idx=0; idx < max_size; idx++)
	{
		if(idx < value_r.size() && !isNull)
		{
			is_null[2*idx+0]=0;
			value_data[2*idx+0] = value_r[idx];
		}
		else
		{
			is_null[2*idx+0]=1;
			value_data[2*idx+0]="";	//useless
		}

		if(idx < value_w.size() && !isNull)
		{
			is_null[2*idx+1]=0;
			value_data[2*idx+1] = value_w[idx];
		}
		else
		{
			is_null[2*idx+1]=1;
			value_data[2*idx+1]="";	//useless
		}

		int_data[5*idx+0] = ID;
		int_data[5*idx+1] = quality;
		error_data[idx] = error_desc;
		error_data_len[idx] = error_data[idx].length();
		if(error_data_len[idx] == 0)
			error_data_is_null[idx] = 1;
		else
			error_data_is_null[idx] = 0;
		int_data[5*idx+2] = idx;
		int_data[5*idx+3] = attr_r_dim.dim_x;	//TODO: missing w sizes
		int_data[5*idx+4] = attr_r_dim.dim_y;	//TODO: missing w sizes
		double_data[2*idx+0] = rcv_time;
		double_data[2*idx+1] = ev_time;


		plog_bind[param_count_single*idx+0].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+0].buffer= (void *)&int_data[5*idx+0];
		plog_bind[param_count_single*idx+0].is_null= 0;
		plog_bind[param_count_single*idx+0].length= 0;

		plog_bind[param_count_single*idx+1].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+1].buffer= (void *)&double_data[2*idx+0];
		plog_bind[param_count_single*idx+1].is_null= 0;
		plog_bind[param_count_single*idx+1].length= 0;

		plog_bind[param_count_single*idx+2].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+2].buffer= (void *)&double_data[2*idx+1];
		plog_bind[param_count_single*idx+2].is_null= 0;
		plog_bind[param_count_single*idx+2].length= 0;

		plog_bind[param_count_single*idx+3].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+3].buffer= (void *)&int_data[5*idx+1];
		plog_bind[param_count_single*idx+3].is_null= 0;
		plog_bind[param_count_single*idx+3].length= 0;

		plog_bind[param_count_single*idx+4].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[param_count_single*idx+4].buffer= (void *)error_data[idx].c_str();	//TODO: escape
		plog_bind[param_count_single*idx+4].is_null= &error_data_is_null[idx];
		error_data_len[idx]=error_data[idx].length();
		plog_bind[param_count_single*idx+4].length= &error_data_len[idx];

		plog_bind[param_count_single*idx+5].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+5].buffer= (void *)&int_data[5*idx+2];
		plog_bind[param_count_single*idx+5].is_null= 0;
		plog_bind[param_count_single*idx+5].length= 0;

		plog_bind[param_count_single*idx+6].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+6].buffer= (void *)&int_data[5*idx+3];
		plog_bind[param_count_single*idx+6].is_null= 0;
		plog_bind[param_count_single*idx+6].length= 0;

		plog_bind[param_count_single*idx+7].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+7].buffer= (void *)&int_data[5*idx+4];
		plog_bind[param_count_single*idx+7].is_null= 0;
		plog_bind[param_count_single*idx+7].length= 0;

		plog_bind[param_count_single*idx+8].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[param_count_single*idx+8].buffer= (void *)value_data[2*idx+0].c_str();
		plog_bind[param_count_single*idx+8].is_null= &is_null[2*idx+0];
		value_data_len[2*idx+0]=value_data[2*idx+0].length();
		plog_bind[param_count_single*idx+8].length= &value_data_len[2*idx+0];

		if(!(write_type == Tango::READ))
		{
			plog_bind[param_count_single*idx+9].buffer_type= MYSQL_TYPE_VARCHAR;
			plog_bind[param_count_single*idx+9].buffer= (void *)value_data[2*idx+1].c_str();
			plog_bind[param_count_single*idx+9].is_null= &is_null[2*idx+1];
			value_data_len[2*idx+1]=value_data[2*idx+1].length();
			plog_bind[param_count_single*idx+9].length= &value_data_len[2*idx+1];
		}
	}

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		cout << __func__<< ": mysql_stmt_bind_param() failed" << endl;
		return -1;
	}

	if (mysql_stmt_execute(pstmt))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << endl;
		delete [] plog_bind;
		return -1;
	}
#ifdef _LIB_DEBUG
	else
	{
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
	}
#endif

/*		if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
	if (mysql_stmt_close(pstmt))
	{
		cout << __func__<< ": failed while closing the statement" << endl;
		delete [] plog_bind;
		return -1;
	}
	delete [] plog_bind;
	return 0;
}

//=============================================================================
//=============================================================================
string HdbPPMySQL::get_only_attr_name(string str)
{
	string::size_type	start = str.find("tango://");
	if (start == string::npos)
		return str;
	else
	{
		start += 8; //	"tango://" length
		start = str.find('/', start);
		start++;
		string	signame = str.substr(start);
		return signame;
	}
}

//=============================================================================
//=============================================================================
string HdbPPMySQL::get_only_tango_host(string str)
{
	string::size_type	start = str.find("tango://");
	if (start == string::npos)
	{
		return "unknown";
	}
	else
	{
		start += 8; //	"tango://" length
		string::size_type	end = str.find('/', start);
		string th = str.substr(start, end-start);
		return th;
	}
}
#ifndef _MULTI_TANGO_HOST
//=============================================================================
//=============================================================================
string HdbPPMySQL::remove_domain(string str)
{
	string::size_type	end1 = str.find(".");
	if (end1 == string::npos)
	{
		return str;
	}
	else
	{
		string::size_type	start = str.find("tango://");
		if (start == string::npos)
		{
			start = 0;
		}
		else
		{
			start = 8;	//tango:// len
		}
		string::size_type	end2 = str.find(":", start);
		if(end1 > end2)	//'.' not in the tango host part
			return str;
		string th = str.substr(0, end1);
		th += str.substr(end2, str.size()-end2);
		return th;
	}
}
#else
/*
//=============================================================================
//=============================================================================
string HdbPPMySQL::remove_domain(string str)
{
	string result="";
	string facility(str);
	vector<string> facilities;
	if(str.find(",") == string::npos)
	{
		facilities.push_back(facility);
	}
	else
	{
		string_explode(facility,",",&facilities);
	}
	for(vector<string>::iterator it = facilities.begin(); it != facilities.end(); it++)
	{
		string::size_type	end1 = it->find(".");
		if (end1 == string::npos)
		{
			result += *it;
			if(it != facilities.end()-1)
				result += ",";
			continue;
		}
		else
		{
			string::size_type	start = it->find("tango://");
			if (start == string::npos)
			{
				start = 0;
			}
			else
			{
				start = 8;	//tango:// len
			}
			string::size_type	end2 = it->find(":", start);
			if(end1 > end2)	//'.' not in the tango host part
			{
				result += *it;
				if(it != facilities.end()-1)
					result += ",";
				continue;
			}
			string th = it->substr(0, end1);
			th += it->substr(end2, it->size()-end2);
			result += th;
			if(it != facilities.end()-1)
				result += ",";
			continue;
		}
	}
	return result;
}
*/
#endif
#ifndef _MULTI_TANGO_HOST
//=============================================================================
//=============================================================================
string HdbPPMySQL::add_domain(string str)
{
	string::size_type	end1 = str.find(".");
	if (end1 == string::npos)
	{
		//get host name without tango://
		string::size_type	start = str.find("tango://");
		if (start == string::npos)
		{
			start = 0;
		}
		else
		{
			start = 8;	//tango:// len
		}
		string::size_type	end2 = str.find(":", start);

		string th = str.substr(start, end2);
		string with_domain = str;;
		struct addrinfo hints;
//		hints.ai_family = AF_INET; // use AF_INET6 to force IPv6
//		hints.ai_flags = AI_CANONNAME|AI_CANONIDN;
		memset(&hints, 0, sizeof hints);
		hints.ai_family = AF_UNSPEC; //either IPV4 or IPV6
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_flags = AI_CANONNAME;
		struct addrinfo *result, *rp;
		int ret = getaddrinfo(th.c_str(), NULL, &hints, &result);
		if (ret != 0)
		{
			fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(ret));
			return str;
		}

		for (rp = result; rp != NULL; rp = rp->ai_next)
		{
			with_domain = string(rp->ai_canonname) + str.substr(end2);
			cout << __func__ <<": found domain -> " << with_domain<<endl;
		}
		freeaddrinfo(result); // all done with this structure
		return with_domain;
	}
	else
	{
		return str;
	}
}
#else
void HdbPPMySQL::string_explode(string str, string separator, vector<string>* results)
{
	string::size_type found;

	found = str.find_first_of(separator);
	while(found != string::npos) {
		if(found > 0) {
			results->push_back(str.substr(0,found));
		}
		str = str.substr(found+1);
		found = str.find_first_of(separator);
	}
	if(str.length() > 0) {
		results->push_back(str);
	}
}
#endif

//=============================================================================
//=============================================================================
string HdbPPMySQL::get_data_type(int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/)
{
	ostringstream data_type;
	if(format==Tango::SCALAR)
	{
		data_type << TYPE_SCALAR << "_";
	}
	else
	{
		data_type << TYPE_ARRAY << "_";
	}

	if(type==Tango::DEV_DOUBLE || type==Tango::DEV_FLOAT)
	{
		data_type << TYPE_DOUBLE << "_";
	}
	else if(type==Tango::DEV_STRING /*|| type==Tango::DEV_STATUS*/)	//TODO: check if exixts
	{
		data_type << TYPE_STRING << "_";
	}
	else if(type==Tango::DEV_LONG || type==Tango::DEV_ULONG || type==Tango::DEV_LONG64 || type==Tango::DEV_ULONG64 || type==Tango::DEV_SHORT || type==Tango::DEV_USHORT)
	{
		data_type << TYPE_I64 << "_";
	}
	else if(type==Tango::DEV_BOOLEAN || type==Tango::DEV_UCHAR || type==Tango::DEV_STATE)
	{
		data_type << TYPE_I8 << "_";
	}
	else	//TODO
	{
		data_type << TYPE_ENCODED << "_";
	}

	if(write_type==Tango::READ)
	{
		data_type << TYPE_RO;
	}
	else
	{
		data_type << TYPE_RW;
	}
	return data_type.str();
}

//=============================================================================
//=============================================================================
string HdbPPMySQL::get_table_name(int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/)
{
	ostringstream table_name;
	table_name << "att_" << get_data_type(type,format,write_type);
	return table_name.str();
}

//=============================================================================
//=============================================================================
AbstractDB* HdbPPMySQLFactory::create_db(string host, string user, string password, string dbname, int port)
{
	return new HdbPPMySQL(host, user, password, dbname, port);
}

//=============================================================================
//=============================================================================
DBFactory *HdbClient::getDBFactory()
{
	HdbPPMySQLFactory *db_mysql_factory = new HdbPPMySQLFactory();
	return static_cast<DBFactory*>(db_mysql_factory);//TODO
}

