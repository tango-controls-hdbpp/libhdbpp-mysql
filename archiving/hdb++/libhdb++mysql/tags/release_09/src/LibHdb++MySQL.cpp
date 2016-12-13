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

#define MYSQL_ERROR		"Mysql Error"
#define CONFIG_ERROR	"Configuration Error"
#define QUERY_ERROR		"Query Error"
#define DATA_ERROR		"Data Error"

#ifndef LIB_BUILDTIME
#define LIB_BUILDTIME   RELEASE " " __DATE__ " "  __TIME__
#endif

const char version_string[] = "$Build: " LIB_BUILDTIME " $";
static const char __FILE__rev[] = __FILE__ " $Id: $";

#define ERR_MAP_MAX_SIZE				50000

//#define _LIB_DEBUG

HdbPPMySQL::HdbPPMySQL(vector<string> configuration)
{
	v_type.push_back(Tango::DEV_BOOLEAN);
	v_type.push_back(Tango::DEV_UCHAR);
	v_type.push_back(Tango::DEV_SHORT);
	v_type.push_back(Tango::DEV_USHORT);
	v_type.push_back(Tango::DEV_LONG);
	v_type.push_back(Tango::DEV_ULONG);
	v_type.push_back(Tango::DEV_LONG64);
	v_type.push_back(Tango::DEV_ULONG64);
	v_type.push_back(Tango::DEV_FLOAT);
	v_type.push_back(Tango::DEV_DOUBLE);
	v_type.push_back(Tango::DEV_STRING);
	v_type.push_back(Tango::DEV_STATE);
	v_type.push_back(Tango::DEV_ENCODED);

	v_format.push_back(Tango::SCALAR);
	v_format.push_back(Tango::SPECTRUM);
	//v_format.push_back<Tango::IMAGE>;

	v_write_type.push_back(Tango::READ);
	v_write_type.push_back(Tango::READ_WRITE);

	lightschema = false;
	autodetectschema = false;
	dbp = new MYSQL();
	if(!mysql_init(dbp))
	{
		stringstream tmp;
		cout << __func__<<": VERSION: " << version_string << " file:" << __FILE__rev << endl;
		tmp << "mysql init db error: "<< mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(MYSQL_ERROR,tmp.str(),__func__);
	}
	my_bool my_auto_reconnect=1;
	if(mysql_options(dbp,MYSQL_OPT_RECONNECT,&my_auto_reconnect) !=0)
	{
		cout << __func__<<": mysql auto reconnection error: " << mysql_error(dbp) << endl;
	}

	map<string,string> db_conf;
	string_vector2map(configuration,"=",&db_conf);
	string host, user, password, dbname;
	int port;
	try
	{
		host = db_conf.at("host");
		user = db_conf.at("user");
		password = db_conf.at("password");
		dbname = db_conf.at("dbname");
		m_dbname = dbname;
		port = atoi(db_conf.at("port").c_str());
	}
	catch(const std::out_of_range& e)
	{
		stringstream tmp;
		tmp << "Configuration parsing error: " << e.what();
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(CONFIG_ERROR,tmp.str(),__func__);
	}
	try
	{
		int ilightschema;
		ilightschema = atoi(db_conf.at("lightschema").c_str());
		lightschema = (ilightschema == 1);
	}
	catch(const std::out_of_range& e)
	{
#ifdef _LIB_DEBUG
		cout << __func__<< ": lightschema key not found" << endl;
#endif
		autodetectschema = true;
	}
	if(!mysql_real_connect(dbp, host.c_str(), user.c_str(), password.c_str(), dbname.c_str(), port, NULL, 0))
	{
		stringstream tmp;
		tmp << "mysql connect db error: "<< mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(MYSQL_ERROR,tmp.str(),__func__);
	}
	else
	{
		//everything OK
#ifdef _LIB_DEBUG
		cout << __func__<< ": mysql connection OK" << endl;
#endif
	}
	if(autodetectschema)
	{
		for(vector<Tango::CmdArgType>::iterator it_type=v_type.begin(); it_type!=v_type.end(); it_type++)
		{
			for(vector<Tango::AttrDataFormat>::iterator it_format=v_format.begin(); it_format!=v_format.end(); it_format++)
			{
				for(vector<Tango::AttrWriteType>::iterator it_write_type=v_write_type.begin(); it_write_type!=v_write_type.end(); it_write_type++)
				{
					if(*it_format == Tango::SCALAR)
					{
						bool detected=autodetect_column(*it_type, *it_format, *it_write_type, SC_COL_INS_TIME);
						table_column_map.insert(make_pair(get_table_name(*it_type, *it_format, *it_write_type)+"_"+SC_COL_INS_TIME, detected));

						detected=autodetect_column(*it_type, *it_format, *it_write_type, SC_COL_RCV_TIME);
						table_column_map.insert(make_pair(get_table_name(*it_type, *it_format, *it_write_type)+"_"+SC_COL_RCV_TIME, detected));

						detected=autodetect_column(*it_type, *it_format, *it_write_type, SC_COL_QUALITY);
						table_column_map.insert(make_pair(get_table_name(*it_type, *it_format, *it_write_type)+"_"+SC_COL_QUALITY, detected));

						detected=autodetect_column(*it_type, *it_format, *it_write_type, SC_COL_ERROR_DESC_ID);
						table_column_map.insert(make_pair(get_table_name(*it_type, *it_format, *it_write_type)+"_"+SC_COL_ERROR_DESC_ID, detected));
					}
					else
					{
						bool detected=autodetect_column(*it_type, *it_format, *it_write_type, ARR_COL_INS_TIME);
						table_column_map.insert(make_pair(get_table_name(*it_type, *it_format, *it_write_type)+"_"+ARR_COL_INS_TIME, detected));

						detected=autodetect_column(*it_type, *it_format, *it_write_type, ARR_COL_RCV_TIME);
						table_column_map.insert(make_pair(get_table_name(*it_type, *it_format, *it_write_type)+"_"+ARR_COL_RCV_TIME, detected));

						detected=autodetect_column(*it_type, *it_format, *it_write_type, ARR_COL_QUALITY);
						table_column_map.insert(make_pair(get_table_name(*it_type, *it_format, *it_write_type)+"_"+ARR_COL_QUALITY, detected));

						detected=autodetect_column(*it_type, *it_format, *it_write_type, ARR_COL_ERROR_DESC_ID);
						table_column_map.insert(make_pair(get_table_name(*it_type, *it_format, *it_write_type)+"_"+ARR_COL_ERROR_DESC_ID, detected));
					}
				}
			}
		}
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
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
			if(row[0])
			{
				ID = atoi(row[0]);
			}
			else	//NOT POSSIBLE!!
			{
#ifdef _LIB_DEBUG
				cout << __func__<< ": ID NULL in query: " << query_str.str() << endl;
#endif
				found = false;
			}

		}	
		mysql_free_result(res);
		if(!found)
			return -1;
	}
	return 0;
}

int HdbPPMySQL::find_attr_id_type(string facility, string attr, int &ID, string attr_type, unsigned int &conf_ttl)
{
	ostringstream query_str;
//	string facility_no_domain = remove_domain(facility);
	string db_type;
#ifndef _MULTI_TANGO_HOST
	query_str << 
		"SELECT " << CONF_TABLE_NAME << "." << CONF_COL_ID << "," << CONF_TYPE_TABLE_NAME << "." << CONF_TYPE_COL_TYPE << "," << CONF_TABLE_NAME << "." << CONF_COL_TTL <<
			" FROM " << m_dbname << "." << CONF_TABLE_NAME <<
			" JOIN " << m_dbname << "." << CONF_TYPE_TABLE_NAME <<
			" ON " << m_dbname << "." << CONF_TABLE_NAME << "." << CONF_COL_TYPE_ID << "=" << m_dbname << "." << CONF_TYPE_TABLE_NAME << "." << CONF_TYPE_COL_TYPE_ID <<
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
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
			if(row[0] == NULL)	//NOT POSSIBLE!!!
			{
#ifdef _LIB_DEBUG
				cout << __func__<< ": ID NULL in query: " << query_str.str() << endl;
#endif
				continue;
			}
			found = true;
			ID = atoi(row[0]);
			db_type = row[1];
            if(row[2])
                    conf_ttl = atoi(row[2]);
            else
                    conf_ttl = 0;
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

int HdbPPMySQL::find_last_event(int ID, string &event)
{
	ostringstream query_str;
	//string facility_no_domain = remove_domain(facility);
	//string facility_with_domain = add_domain(facility);

	query_str <<
		"SELECT " << HISTORY_EVENT_COL_EVENT <<
			" FROM " << m_dbname << "." << HISTORY_TABLE_NAME <<
			" JOIN " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME <<
			" ON " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME << "." << HISTORY_EVENT_COL_EVENT_ID << "=" << m_dbname << "." << HISTORY_TABLE_NAME << "." << HISTORY_COL_EVENT_ID <<
			" WHERE " << HISTORY_COL_ID << " = " << ID <<
			" ORDER BY " << HISTORY_COL_TIME << " DESC LIMIT 1";


	if(mysql_query(dbp, query_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
			event = row[0];
		}
		mysql_free_result(res);
		if(!found)
			return -1;
	}
	return 0;
}

int HdbPPMySQL::find_err_id(string err, int &ERR_ID)
{
	char err_escaped[2 * err.length() + 1];
	mysql_escape_string(err_escaped, err.c_str(), err.length());
	ostringstream query_str;
	//string facility_no_domain = remove_domain(facility);
	//string facility_with_domain = add_domain(facility);
	query_str <<
		"SELECT " << ERR_COL_ID << " FROM " << m_dbname << "." << ERR_TABLE_NAME <<
			" WHERE " << ERR_COL_ERROR_DESC << " = '" << err_escaped << "'";

	if(mysql_query(dbp, query_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
#ifdef _LIB_DEBUG
			cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
#endif
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
			if(row[0])
			{
				ERR_ID = atoi(row[0]);
			}
			else //NOT POSSIBLE!!
			{
#ifdef _LIB_DEBUG
				cout << __func__<< ": ID NULL in query: " << query_str.str() << endl;
#endif
				found = false;
			}
		}
		mysql_free_result(res);
		if(!found)
			return -1;
	}
	return 0;
}

void HdbPPMySQL::cache_err_id(string error_desc, int &ERR_ID)
{
	ERR_ID=-1;
	if(error_desc.length() == 0)
		return;
#ifdef _LIB_DEBUG
	cout << __func__<< ": entering for '"<<error_desc << "' map size=" << attr_ERR_queue.size() << endl;
#endif
	map<string,int>::iterator it = attr_ERR_ID_map.find(error_desc);
	//if not already present in cache, look for ID in the DB
	if(it == attr_ERR_ID_map.end())
	{
		find_err_id(error_desc, ERR_ID);
		if(ERR_ID != -1)
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": '"<<error_desc << "' found in table, ERR_ID="<<ERR_ID << endl;
#endif
			attr_ERR_ID_map.insert(make_pair(error_desc,ERR_ID));
			attr_ERR_queue.push(error_desc);
			while(attr_ERR_queue.size() > ERR_MAP_MAX_SIZE)
			{
				attr_ERR_ID_map.erase(attr_ERR_queue.front());
				attr_ERR_queue.pop();
			}
			it = attr_ERR_ID_map.find(error_desc);
		}
	}
	if(it == attr_ERR_ID_map.end())
	{
#ifdef _LIB_DEBUG
		cout << __func__<< ": ERR_ID not found for err='"<<error_desc << "' inserting new one" << endl;
#endif
		insert_error(error_desc, ERR_ID);
		if(ERR_ID != -1)
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": '"<<error_desc << "' INSERTED in table, ERR_ID="<<ERR_ID << endl;
#endif
			attr_ERR_ID_map.insert(make_pair(error_desc,ERR_ID));
			attr_ERR_queue.push(error_desc);
			while(attr_ERR_queue.size() > ERR_MAP_MAX_SIZE)
			{
				attr_ERR_ID_map.erase(attr_ERR_queue.front());
				attr_ERR_queue.pop();
			}
		}
	}
	else
	{
		ERR_ID=it->second;
	}
#ifdef _LIB_DEBUG
	cout << __func__<< ": exiting for '"<<error_desc << "' ERR_ID="<<ERR_ID<<" map size=" << attr_ERR_queue.size() << endl;
#endif
}

int HdbPPMySQL::insert_error(string error_desc, int &ERR_ID)
{
	ERR_ID = -1;
	ostringstream query_str;
	query_str <<
		"INSERT INTO " << m_dbname << "." << ERR_TABLE_NAME <<
			" (" << ERR_COL_ERROR_DESC << ")";

	query_str << " VALUES (?)";

	MYSQL_STMT	*pstmt;
	MYSQL_BIND	plog_bind[1];
	string		param_data[1];
	unsigned long param_data_len[1];
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		stringstream tmp;
		tmp << "mysql_stmt_init(), out of memory";
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		stringstream tmp;
		tmp << "mysql_stmt_prepare(), INSERT failed query=" << query_str.str() << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	param_data[0] = error_desc;
	param_data_len[0] = error_desc.length();

	memset(plog_bind, 0, sizeof(plog_bind));

	plog_bind[0].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[0].buffer= (void *)param_data[0].c_str();
	plog_bind[0].is_null= 0;
	plog_bind[0].length= &param_data_len[0];

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		stringstream tmp;
		tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	if (mysql_stmt_execute(pstmt))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
	}
	else
	{
#ifdef _LIB_DEBUG
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
		ostringstream query_last_str;
		query_last_str <<
			"SELECT LAST_INSERT_ID() FROM " << m_dbname << "." << ERR_TABLE_NAME;

		if(mysql_query(dbp, query_last_str.str().c_str()))
		{
			stringstream tmp;
			tmp << "ERROR in query=" << query_last_str.str() << ", err=" << mysql_error(dbp);
			cout << __func__<< ": " << tmp.str() << endl;
			//Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
				cout << __func__<< ": NO RESULT in query: " << query_last_str.str() << endl;
			}
			else
			{
				while ((row = mysql_fetch_row(res)))
				{
					if(row[0])
					{
						ERR_ID = atoi(row[0]);
#ifdef _LIB_DEBUG
						cout << __func__<< ": found last id for '"<<error_desc << "' ERR_ID="<<ERR_ID<< endl;
#endif
						break;
					}
					else //NOT POSSIBLE!!!
					{
#ifdef _LIB_DEBUG
						cout << __func__<< ": NOT found last id for '"<<error_desc << "' ERR_ID=NULL"<< endl;
#endif
					}
				}
				mysql_free_result(res);
			}
		}
	}
	if (mysql_stmt_close(pstmt))
	{
		stringstream tmp;
		tmp << "failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
	return 0;
}

int HdbPPMySQL::insert_Attr(Tango::EventData *data, HdbEventDataType ev_data_type)
{
	int ret = -1;
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif

	string attr_name = data->attr_name;
	double	ev_time;
	double	rcv_time = data->get_date().tv_sec + (double)data->get_date().tv_usec/1.0e6;
	int quality = (int)data->attr_value->get_quality();
	string error_desc("");
#ifdef _LIB_DEBUG
	cout << __func__<< ": entering quality="<<quality << endl;
#endif

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
	//int max_dim_x = ev_data_type.max_dim_x;
	//int max_dim_y = ev_data_type.max_dim_y;
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
		attr_r_dim.dim_x = 0;//max_dim_x;//TODO: OK?
		attr_w_dim.dim_x = 0;//max_dim_x;//TODO: OK?
		attr_r_dim.dim_y = 0;//max_dim_y;//TODO: OK?
		attr_w_dim.dim_y = 0;//max_dim_y;//TODO: OK?
	}
	if(ev_time < 1)
		ev_time=1;
	if(rcv_time < 1)
		rcv_time=1;

	string table_name = get_table_name(data_type, data_format, write_type);

	switch(data_type)
	{
		case Tango::DEV_DOUBLE:
		{
			ret = extract_and_store<Tango::DevDouble>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_DOUBLE, false/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_FLOAT:
		{
			ret = extract_and_store<Tango::DevFloat>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_FLOAT, false/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_LONG:
		{
			ret = extract_and_store<Tango::DevLong>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONG, false/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_ULONG:
		{
			ret = extract_and_store<Tango::DevULong>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONG, true/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_LONG64:
		{
			ret = extract_and_store<Tango::DevLong64>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG, false/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_ULONG64:
		{
			ret = extract_and_store<Tango::DevULong64>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_LONGLONG, true/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_SHORT:
		{
			ret = extract_and_store<Tango::DevShort>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_SHORT, false/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_USHORT:
		{
			ret = extract_and_store<Tango::DevUShort>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_SHORT, true/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_BOOLEAN:
		{
			ret = extract_and_store<Tango::DevBoolean>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY, true/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_UCHAR:
		{
			ret = extract_and_store<Tango::DevUChar>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY, true/*is_unsigned*/, isNull);
			break;
		}
		case Tango::DEV_STRING:
		{
			vector<string>	vsval_r;
			vector<string>	vsval_w;
			if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(vsval_r)) ||
				(write_type != Tango::READ && data->attr_value->extract_read(vsval_r) && data->attr_value->extract_set(vsval_w))))	//TODO: WO
			{
				if(data_format == Tango::SCALAR)
					ret = store_scalar_string(attr_name, vsval_r, vsval_w, quality, error_desc, data_type, write_type, ev_time, rcv_time, table_name);
				else
					ret = store_array_string(attr_name, vsval_r, vsval_w, quality, error_desc, data_type, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name);
			}
			else
			{
				vsval_r.push_back(""); //fake value
				if(data_format == Tango::SCALAR)
					ret = store_scalar_string(attr_name, vsval_r, vsval_w, quality, error_desc, data_type, write_type, ev_time, rcv_time, table_name, true);
				else
					ret = store_array_string(attr_name, vsval_r, vsval_w, quality, error_desc, data_type, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, true);
				if(!isNull)
					cout << __func__<<": failed to extract " << attr_name << endl;
				return ret;
			}
			break;
		}
		case Tango::DEV_STATE:
		{
#if 1
			vector<Tango::DevState>	vstval_r;
			vector<Tango::DevState>	vstval_w;
			if(write_type == Tango::READ && data_format == Tango::SCALAR)
			{
				// We cannot use the extract_read() method for the "State" attribute
				Tango::DevState	st;
				if(!isNull)
				{
					*data->attr_value >> st;
				}
				else
				{
					st = (Tango::DevState)0; //fake value
				}
				vstval_r.push_back(st);
				ret = store_scalar<Tango::DevState>(attr_name, vstval_r, vstval_w, quality, error_desc, data_type, write_type, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY, true/*is_unsigned*/, isNull);
				return ret;
			}
			else
			{
				ret = extract_and_store<Tango::DevState>(attr_name, data, quality, error_desc, data_type, data_format, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY, true/*is_unsigned*/, isNull);
			}
#else	//TODO: extract_read fails on state attribute
			ret = extract_and_store<Tango::DevState>(attr_name, data, quality, error_desc, data_format, data_type, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, MYSQL_TYPE_TINY, true/*is_unsigned*/, isNull);
#endif
			break;
		}
		default:
		{
			TangoSys_MemStream	os;
			os << "Attribute " << data->attr_name<< " type (" << data_type << "<->" << (int)(data->attr_value->get_type()) << ") not supported";
			cout << __func__<<": " << os.str() << endl;
			Tango::Except::throw_exception(DATA_ERROR,os.str(),__func__);
		}
	}
#ifdef _LIB_DEBUG
//	cout << __func__<< ": exiting... ret="<<ret << endl;
#endif
	return ret;
}

template <typename Type> int HdbPPMySQL::extract_and_store(string attr_name, Tango::EventData *data, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, Tango::AttrDataFormat data_format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool _is_unsigned, bool isNull)
{
	int ret;
	vector<Type>	val_r;
	vector<Type>	val_w;
	if(!isNull && ((write_type == Tango::READ && data->attr_value->extract_read(val_r)) ||
		(write_type != Tango::READ && data->attr_value->extract_read(val_r) && data->attr_value->extract_set(val_w))))
	{
		if(data_format == Tango::SCALAR)
			ret = store_scalar<Type>(attr_name, val_r, val_w, quality, error_desc, data_type, write_type, ev_time, rcv_time, table_name, mysql_value_type, _is_unsigned);
		else
			ret = store_array<Type>(attr_name, val_r, val_w, quality, error_desc, data_type, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, mysql_value_type, _is_unsigned);
	}
	else
	{
		val_r.push_back((Type)0); //fake value
		if(data_format == Tango::SCALAR)
			ret = store_scalar<Type>(attr_name, val_r, val_w, quality, error_desc, data_type, write_type, ev_time, rcv_time, table_name, mysql_value_type, _is_unsigned, true);
		else
			ret = store_array<Type>(attr_name, val_r, val_w, quality, error_desc, data_type, write_type, attr_r_dim, attr_w_dim, ev_time, rcv_time, table_name, mysql_value_type, _is_unsigned, true);
		if(!isNull)
			cout << __func__<<": failed to extract " << attr_name << endl;
	}
	return ret;
}

int HdbPPMySQL::insert_param_Attr(Tango::AttrConfEventData *data, HdbEventDataType ev_data_type)
{
	int ret = -1;
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif

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
			Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found for attr="<<attr << endl;
		Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
	}
	int ID=it->second;
	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << PARAM_TABLE_NAME <<
			" (" << PARAM_COL_ID << "," << PARAM_COL_INS_TIME << "," << PARAM_COL_EV_TIME << "," <<
			PARAM_COL_LABEL << "," << PARAM_COL_UNIT << "," << PARAM_COL_STANDARDUNIT << "," <<
			PARAM_COL_DISPLAYUNIT << "," << PARAM_COL_FORMAT << "," << PARAM_COL_ARCHIVERELCHANGE << "," <<
			PARAM_COL_ARCHIVEABSCHANGE << "," << PARAM_COL_ARCHIVEPERIOD << "," << PARAM_COL_DESCRIPTION << ")";

	query_str << " VALUES (?,NOW(6),FROM_UNIXTIME(?)," <<
			"?,?,?," <<
			"?,?,?," <<
			"?,?,?)" ;

	MYSQL_STMT	*pstmt;
	MYSQL_BIND	plog_bind[11];
	double		double_data;
	int			int_data;
	string		param_data[9];
	unsigned long param_data_len[9];
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		stringstream tmp;
		tmp << "mysql_stmt_init(), out of memory";
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		stringstream tmp;
		tmp << "mysql_stmt_prepare(), INSERT failed query=" << query_str.str() << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
	if(data->attr_conf->description.length() <= 1024)
	{
		param_data[8] = data->attr_conf->description;
	}
	else
	{
		param_data[8] = data->attr_conf->description.substr(0,1023);
	}
	param_data_len[8] = param_data[8].length();

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
	plog_bind[2].buffer= (void *)param_data[0].c_str();
	plog_bind[2].is_null= 0;
	plog_bind[2].length= &param_data_len[0];

	plog_bind[3].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[3].buffer= (void *)param_data[1].c_str();
	plog_bind[3].is_null= 0;
	plog_bind[3].length= &param_data_len[1];

	plog_bind[4].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[4].buffer= (void *)param_data[2].c_str();
	plog_bind[4].is_null= 0;
	plog_bind[4].length= &param_data_len[2];

	plog_bind[5].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[5].buffer= (void *)param_data[3].c_str();
	plog_bind[5].is_null= 0;
	plog_bind[5].length= &param_data_len[3];

	plog_bind[6].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[6].buffer= (void *)param_data[4].c_str();
	plog_bind[6].is_null= 0;
	plog_bind[6].length= &param_data_len[4];

	plog_bind[7].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[7].buffer= (void *)param_data[5].c_str();
	plog_bind[7].is_null= 0;
	plog_bind[7].length= &param_data_len[5];

	plog_bind[8].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[8].buffer= (void *)param_data[6].c_str();
	plog_bind[8].is_null= 0;
	plog_bind[8].length= &param_data_len[6];

	plog_bind[9].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[9].buffer= (void *)param_data[7].c_str();
	plog_bind[9].is_null= 0;
	plog_bind[9].length= &param_data_len[7];

	plog_bind[10].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[10].buffer= (void *)param_data[8].c_str();
	plog_bind[10].is_null= 0;
	plog_bind[10].length= &param_data_len[8];

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		stringstream tmp;
		tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	if (mysql_stmt_execute(pstmt))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
#ifdef _LIB_DEBUG
	else
	{
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
	}
#endif
	if (mysql_stmt_close(pstmt))
	{
		stringstream tmp;
		tmp << "failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

#ifdef _LIB_DEBUG
//	cout << __func__<< ": exiting... ret="<<ret << endl;
#endif
	return ret;
}

int HdbPPMySQL::configure_Attr(string name, int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/, unsigned int ttl/*hours, 0=infinity*/)
{
	ostringstream insert_str;
	ostringstream insert_event_str;
	ostringstream update_ttl_str;
	string facility = get_only_tango_host(name);
#ifndef _MULTI_TANGO_HOST
	facility = add_domain(facility);
#endif
	string attr_name = get_only_attr_name(name);
	cout<< __func__ << ": name="<<name<<" -> facility="<<facility<<" attr_name="<<attr_name<< endl;
	int id=-1;
	string data_type = get_data_type(type, format, write_type);
	unsigned int conf_ttl=0;
	int ret = find_attr_id_type(facility, attr_name, id, data_type, conf_ttl);
	//ID already present but different configuration (attribute type)
	if(ret == -2)
	{
		stringstream tmp;
		tmp << "ERROR "<<facility<<"/"<<attr_name<<" already configured with ID="<<id;
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(DATA_ERROR,tmp.str(),__func__);
	}

	//ID found and same configuration (attribute type): do nothing
	if(ret == 0)
	{
#ifdef _LIB_DEBUG
		cout<< __func__ << ": ALREADY CONFIGURED with same configuration: "<<facility<<"/"<<attr_name<<" with ID="<<id << endl;
#endif
		if(conf_ttl != ttl)
		{
#ifdef _LIB_DEBUG
			cout<< __func__ << ": .... BUT different ttl: updating " << conf_ttl << " to " << ttl << endl;
#endif
			update_ttl_str <<
				"UPDATE " << m_dbname << "." << CONF_TABLE_NAME << " SET " <<
					CONF_COL_TTL << "=" << ttl <<
					" WHERE " << CONF_COL_ID << "=" << id;

			if(mysql_query(dbp, update_ttl_str.str().c_str()))
			{
				stringstream tmp;
				tmp << "ERROR in query=" << update_ttl_str.str() << ", err=" << mysql_error(dbp);
				cout << __func__<< ": " << tmp.str() << endl;
				Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
			}
		}
		insert_event_str <<
			"INSERT INTO " << m_dbname << "." << HISTORY_TABLE_NAME << " ("<<HISTORY_COL_ID<<","<<HISTORY_COL_EVENT_ID<<","<<HISTORY_COL_TIME<<")" <<
				" SELECT " << id << "," << HISTORY_EVENT_COL_EVENT_ID << ",NOW(6)" <<
				" FROM " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME << " WHERE " << HISTORY_EVENT_COL_EVENT << " = '" << EVENT_ADD << "'";

		if(mysql_query(dbp, insert_event_str.str().c_str()))
		{
			stringstream tmp;
			tmp << "ERROR in query=" << insert_event_str.str() << ", err=" << mysql_error(dbp);
			cout << __func__<< ": " << tmp.str() << endl;
			Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
		}
		return 0;
	}

	//add domain name to fqdn
	name = string("tango://")+facility+string("/")+attr_name;
	char name_escaped[2 * name.length() + 1];
	mysql_escape_string(name_escaped, name.c_str(), name.length());

	vector<string> exploded_name;
	string_explode(attr_name,"/",&exploded_name);

	string complete_facility=string("tango://")+facility;
	char complete_facility_escaped[2 * complete_facility.length() + 1];
	mysql_escape_string(complete_facility_escaped, complete_facility.c_str(), complete_facility.length());

	string domain="";
	string family="";
	string member="";
	string last_name="";
	if(exploded_name.size() == 4)
	{
		domain=exploded_name[0];
		family=exploded_name[1];
		member=exploded_name[2];
		last_name=exploded_name[3];
	}
	else
	{
		cout<< __func__ << ": FAILED to explode " << attr_name << " into 4 fields, result is " << exploded_name.size() << endl;
	}
	char domain_escaped[2 * domain.length() + 1];
	mysql_escape_string(domain_escaped, domain.c_str(), domain.length());
	char family_escaped[2 * family.length() + 1];
	mysql_escape_string(family_escaped, family.c_str(), family.length());
	char member_escaped[2 * member.length() + 1];
	mysql_escape_string(member_escaped, member.c_str(), member.length());
	char last_name_escaped[2 * last_name.length() + 1];
	mysql_escape_string(last_name_escaped, last_name.c_str(), last_name.length());

	insert_str <<
		"INSERT INTO " << m_dbname << "." << CONF_TABLE_NAME << " ("<<CONF_COL_NAME<<","<<CONF_COL_TYPE_ID<<","<<CONF_COL_TTL<<","<<
			CONF_COL_FACILITY<<","<<CONF_COL_DOMAIN<<","<<CONF_COL_FAMILY<<","<<CONF_COL_MEMBER<<","<<CONF_COL_LAST_NAME<<")"<<
			" SELECT '" << name_escaped << "'," << CONF_TYPE_COL_TYPE_ID << "," << ttl <<
			",'"<<complete_facility_escaped<<"','"<<domain_escaped<<"','"<<family_escaped<<"','"<<member_escaped<<"','"<<last_name_escaped<<"'"<<
			" FROM " << m_dbname << "." << CONF_TYPE_TABLE_NAME << " WHERE " << CONF_TYPE_COL_TYPE << " = '" << data_type << "'";

	if(mysql_query(dbp, insert_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << insert_str.str() << ", err=" << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	//int last_id = mysql_insert_id(dbp);

	insert_event_str <<
		"INSERT INTO " << m_dbname << "." << HISTORY_TABLE_NAME << " ("<<HISTORY_COL_ID<<","<<HISTORY_COL_EVENT_ID<<","<<HISTORY_COL_TIME<<")" <<
			" SELECT LAST_INSERT_ID()," << HISTORY_EVENT_COL_EVENT_ID << ",NOW(6)" <<
			" FROM " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME << " WHERE " << HISTORY_EVENT_COL_EVENT << " = '" << EVENT_ADD << "'";

	if(mysql_query(dbp, insert_event_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << insert_event_str.str() << ", err=" << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	return 0;
}

int HdbPPMySQL::event_Attr(string name, unsigned char event)
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
		stringstream tmp;
		tmp << "ERROR "<<facility<<"/"<<attr_name<<" NOT FOUND";
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(DATA_ERROR,tmp.str(),__func__);
	}

	insert_event_str <<
		"INSERT INTO " << m_dbname << "." << HISTORY_TABLE_NAME << " ("<<HISTORY_COL_ID<<","<<HISTORY_COL_EVENT_ID<<","<<HISTORY_COL_TIME<<")" <<
			" SELECT " << id << "," << HISTORY_EVENT_COL_EVENT_ID << ",NOW(6)" <<
			" FROM " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME << " WHERE " << HISTORY_EVENT_COL_EVENT << " = '";

	switch(event)
	{
		case DB_START:
		{
			string last_event;
			ret = find_last_event(id, last_event);
			if(ret == 0 && last_event == EVENT_START)
			{
				ostringstream insert_event_crash_str;
				insert_event_crash_str <<
					"INSERT INTO " << m_dbname << "." << HISTORY_TABLE_NAME << " ("<<HISTORY_COL_ID<<","<<HISTORY_COL_EVENT_ID<<","<<HISTORY_COL_TIME<<")" <<
						" SELECT " << id << "," << HISTORY_EVENT_COL_EVENT_ID << ",NOW(6)" <<
						" FROM " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME << " WHERE " << HISTORY_EVENT_COL_EVENT << " = '" << EVENT_CRASH << "'";

				if(mysql_query(dbp, insert_event_crash_str.str().c_str()))
				{
					cout<< __func__ << ": ERROR in query=" << insert_event_crash_str.str() << endl;
				}
			}

			insert_event_str << EVENT_START << "'";
			break;
		}
		case DB_STOP:
		{
			insert_event_str << EVENT_STOP << "'";
			break;
		}
		case DB_REMOVE:
		{
			insert_event_str << EVENT_REMOVE << "'";
			break;
		}
		case DB_PAUSE:
		{
			insert_event_str << EVENT_PAUSE << "'";
			break;
		}
		default:
		{
			stringstream tmp;
			tmp << "ERROR for "<<facility<<"/"<<attr_name<<" event=" << (int)event << " NOT SUPPORTED";
			cout << __func__<< ": " << tmp.str() << endl;
			Tango::Except::throw_exception(DATA_ERROR,tmp.str(),__func__);
		}
	}

	if(mysql_query(dbp, insert_event_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << insert_event_str.str() << ", err=" << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(DATA_ERROR,tmp.str(),__func__);
	}

	return 0;
}

//=============================================================================
//=============================================================================
template <typename Type> int HdbPPMySQL::store_scalar(string attr, vector<Type> value_r, vector<Type> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool _is_unsigned, bool isNull)
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
			Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found for attr="<<attr << endl;
		Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
	}
	int ID=it->second;

	bool detected_insert_time = true;
	bool detected_recv_time = true;
	bool detected_quality = true;
	bool detected_error = true;
	if(autodetectschema)
	{
		try
		{
			detected_insert_time = table_column_map.at(get_table_name(data_type, Tango::SCALAR, write_type)+"_"+SC_COL_INS_TIME);
		}
		catch(std::out_of_range &e)
		{
			detected_insert_time = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...SC_COL_INS_TIME) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_recv_time = table_column_map.at(get_table_name(data_type, Tango::SCALAR, write_type)+"_"+SC_COL_RCV_TIME);
		}
		catch(std::out_of_range &e)
		{
			detected_recv_time = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...SC_COL_RCV_TIME) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_quality = table_column_map.at(get_table_name(data_type, Tango::SCALAR, write_type)+"_"+SC_COL_QUALITY);
		}
		catch(std::out_of_range &e)
		{
			detected_quality = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...SC_COL_QUALITY) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_error = table_column_map.at(get_table_name(data_type, Tango::SCALAR, write_type)+"_"+SC_COL_ERROR_DESC_ID);
		}
		catch(std::out_of_range &e)
		{
			detected_error = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...SC_COL_ERROR_DESC) NOT FOUND" << endl;
#endif
		}
	}
	else if(lightschema)
	{
		detected_insert_time = false;
		detected_recv_time = false;
	}
	int ERR_ID=-1;
	if(detected_error)
		cache_err_id(error_desc, ERR_ID);

	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << table_name <<
			" (" << SC_COL_ID << "," << SC_COL_EV_TIME << ",";
	if(detected_insert_time)
		query_str << SC_COL_INS_TIME << ",";
	if(detected_recv_time)
		query_str << SC_COL_RCV_TIME << ",";
	if(detected_quality)
		query_str << SC_COL_QUALITY << ",";
	if(detected_error)
		query_str << SC_COL_ERROR_DESC_ID << ",";
	query_str << SC_COL_VALUE_R;
	if(!(write_type == Tango::READ))	//RW
		query_str << "," << SC_COL_VALUE_W;
	query_str << ")";

	query_str << " VALUES (?,FROM_UNIXTIME(?),";
	if(detected_insert_time)
		query_str << "NOW(6),";//insert_time
	if(detected_recv_time)
		query_str << "FROM_UNIXTIME(?),";//recv_time
	if(detected_quality)
		query_str << "?,";//quality
	if(detected_error)
		query_str << "?,";//error
	query_str << "?";	//value_r
	if(!(write_type == Tango::READ))	//RW
		query_str << ",?";//value_w
	query_str << ")";

	MYSQL_STMT	*pstmt;
	MYSQL_BIND	plog_bind[7];
	my_bool		is_null[3];    /* value nullability */
	my_bool		is_unsigned=_is_unsigned;    /* value unsigned */
	double		double_data[2];
	Type		value_data[2];
	int			int_data[3];
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		Tango::Except::throw_exception(QUERY_ERROR,"mysql_stmt_init(): out of memory",__func__);
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		stringstream tmp;
		tmp << "mysql_stmt_prepare(), INSERT failed" << ", err='" << mysql_stmt_error(pstmt) << "' query='" << query_str.str() << "'";
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	if(value_r.size() >= 1 && !isNull)
	{
		if(is_nan_or_inf(value_r[0]))
		{
			is_null[0]=1;
			value_data[0]=(Type)0;	//useless
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
		value_data[0]=(Type)0;	//useless
	}

	if(value_w.size() >= 1 && !isNull)
	{
		if(is_nan_or_inf(value_w[0]))
		{
			is_null[1]=1;
			value_data[1]=(Type)0;	//useless
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
		value_data[1]=(Type)0;	//useless
	}

	int_data[0] = ID;
	if(detected_quality)
		int_data[1] = quality;
	double_data[0] = ev_time;
	if(detected_recv_time)
		double_data[1] = rcv_time;
	if(detected_error)
	{
		int_data[2] = ERR_ID;
		if(ERR_ID < 0)
			is_null[2]=1;
		else
			is_null[2]=0;
	}
	memset(plog_bind, 0, sizeof(plog_bind));

	size_t plog_bind_ind=0;

	plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
	plog_bind[plog_bind_ind].buffer= (void *)&int_data[0];
	plog_bind[plog_bind_ind].is_null= 0;
	plog_bind[plog_bind_ind].length= 0;
	plog_bind_ind++;

	plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
	plog_bind[plog_bind_ind].buffer= (void *)&double_data[0];
	plog_bind[plog_bind_ind].is_null= 0;
	plog_bind[plog_bind_ind].length= 0;
	plog_bind_ind++;

	if(detected_recv_time)
	{
		plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[plog_bind_ind].buffer= (void *)&double_data[1];
		plog_bind[plog_bind_ind].is_null= 0;
		plog_bind[plog_bind_ind].length= 0;
		plog_bind_ind++;
	}
	if(detected_quality)
	{
		plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[plog_bind_ind].buffer= (void *)&int_data[1];
		plog_bind[plog_bind_ind].is_null= 0;
		plog_bind[plog_bind_ind].length= 0;
		plog_bind_ind++;
	}

	if(detected_error)
	{
		plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[plog_bind_ind].buffer= (void *)&int_data[2];
		plog_bind[plog_bind_ind].is_null= &is_null[2];
		plog_bind[plog_bind_ind].length= 0;
		plog_bind[plog_bind_ind].is_unsigned= 1;
		plog_bind_ind++;
	}

	plog_bind[plog_bind_ind].buffer_type= mysql_value_type;
	plog_bind[plog_bind_ind].buffer= (void *)&value_data[0];
	plog_bind[plog_bind_ind].is_null= &is_null[0];
	plog_bind[plog_bind_ind].is_unsigned= is_unsigned;
	plog_bind[plog_bind_ind].length= 0;
	plog_bind_ind++;

	plog_bind[plog_bind_ind].buffer_type= mysql_value_type;
	plog_bind[plog_bind_ind].buffer= (void *)&value_data[1];
	plog_bind[plog_bind_ind].is_null= &is_null[1];
	plog_bind[plog_bind_ind].is_unsigned= is_unsigned;
	plog_bind[plog_bind_ind].length= 0;
	plog_bind_ind++;

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		stringstream tmp;
		tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	if (mysql_stmt_execute(pstmt))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_stmt_error(pstmt);
		cout<< __func__ << ": " << tmp.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt) << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
		stringstream tmp;
		tmp << "failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt);
		cout<< __func__ << ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	return 0;
}

//=============================================================================
//=============================================================================
template <typename Type> int HdbPPMySQL::store_array(string attr, vector<Type> value_r, vector<Type> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, enum_field_types mysql_value_type, bool _is_unsigned, bool isNull)
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
			Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found for attr="<<attr << endl;
		Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
	}
	int ID=it->second;
	uint32_t max_size = (value_r.size() > value_w.size()) ? value_r.size() : value_w.size();

	bool detected_insert_time = true;
	bool detected_recv_time = true;
	bool detected_quality = true;
	bool detected_error = true;
	if(autodetectschema)
	{
		try
		{
			detected_insert_time = table_column_map.at(get_table_name(data_type, Tango::SPECTRUM, write_type)+"_"+ARR_COL_INS_TIME);
		}
		catch(std::out_of_range &e)
		{
			detected_insert_time = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...ARR_COL_INS_TIME) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_recv_time = table_column_map.at(get_table_name(data_type, Tango::SPECTRUM, write_type)+"_"+ARR_COL_RCV_TIME);
		}
		catch(std::out_of_range &e)
		{
			detected_recv_time = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...ARR_COL_RCV_TIME) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_quality = table_column_map.at(get_table_name(data_type, Tango::SPECTRUM, write_type)+"_"+ARR_COL_QUALITY);
		}
		catch(std::out_of_range &e)
		{
			detected_quality = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...ARR_COL_QUALITY) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_error = table_column_map.at(get_table_name(data_type, Tango::SPECTRUM, write_type)+"_"+ARR_COL_ERROR_DESC_ID);
		}
		catch(std::out_of_range &e)
		{
			detected_error = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...ARR_COL_ERROR_DESC) NOT FOUND" << endl;
#endif
		}
	}
	else if(lightschema)
	{
		detected_insert_time = false;
		detected_recv_time = false;
	}
	int ERR_ID=-1;
	if(detected_error)
		cache_err_id(error_desc, ERR_ID);

	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << table_name <<
			" (" << ARR_COL_ID << "," << SC_COL_EV_TIME << ",";
	if(detected_insert_time)
		query_str << SC_COL_INS_TIME << ",";
	if(detected_recv_time)
		query_str << SC_COL_RCV_TIME << ",";
	if(detected_quality)
		query_str << ARR_COL_QUALITY << ",";
	if(detected_error)
		query_str << ARR_COL_ERROR_DESC_ID << ",";
	query_str << ARR_COL_IDX << "," << ARR_COL_DIMX_R << "," << ARR_COL_DIMY_R << "," << ARR_COL_VALUE_R;
	if(!(write_type == Tango::READ))	//RW
		query_str << "," << ARR_COL_DIMX_W << "," << ARR_COL_DIMY_W << "," << ARR_COL_VALUE_W;
	query_str << ")";

	query_str << " VALUES ";
	for(size_t idx=0; idx < max_size; idx++)
	{
		query_str << " (?,FROM_UNIXTIME(?),";
		if(detected_insert_time)
			query_str << "NOW(6),";
		if(detected_recv_time)
			query_str << "FROM_UNIXTIME(?),";
		if(detected_quality)
			query_str << "?,";
		if(detected_error)
			query_str << "?,";
		query_str << "?,?,?,?";
		if(!(write_type == Tango::READ))	//RW
			query_str << ",?,?,?";
		query_str << ")";
		if(idx < max_size-1)
			query_str << ",";
	}
	if(max_size == 0)
	{
		query_str << "(?,FROM_UNIXTIME(?),";
		if(detected_insert_time)
			query_str << "NOW(6),";
		if(detected_recv_time)
			query_str << "FROM_UNIXTIME(?),";
		if(detected_quality)
			query_str << "?,";
		if(detected_error)
			query_str << "?,";
		query_str << "?,?,?,?";
		if(!(write_type == Tango::READ))	//RW
			query_str << ",?,?,?";
		query_str << ")";
	}
	int param_count_single = 6;	//param in single value insert
	if(detected_recv_time)
		param_count_single++;
	if(detected_quality)
		param_count_single++;
	if(detected_error)
		param_count_single++;
	if(write_type != Tango::READ)
		param_count_single += 3;
	int param_count = param_count_single*max_size;								//total param
	MYSQL_STMT	*pstmt;
	MYSQL_BIND	*plog_bind = new MYSQL_BIND[param_count];
	my_bool		is_null[3*max_size];    /* value nullability */	//value_r, value_w, error_desc_id
	my_bool		is_unsigned=_is_unsigned;    /* value unsigned */
	double		double_data[2*max_size];	// rcv_time, ev_time
	Type		value_data[2*max_size];		//value_r, value_w
	int			int_data[8*max_size];		//id, quality, error_desc_id, idx, dimx_r, dimy_r, dimx_w, dimy_w,
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		Tango::Except::throw_exception(QUERY_ERROR,"mysql_stmt_init(): out of memory",__func__);
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		stringstream tmp;
		tmp << "mysql_stmt_prepare(), prepare stmt failed, stmt='"<<query_str.str()<<"', err="<<mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
	memset(plog_bind, 0, sizeof(MYSQL_BIND)*param_count);

	for(size_t idx=0; idx < max_size; idx++)
	{
		if(idx < value_r.size() && !isNull)
		{
			if(is_nan_or_inf(value_r[idx]))
			{
				is_null[3*idx+0]=1;
				value_data[2*idx+0]=(Type)0;	//useless
			}
			else
			{
				is_null[3*idx+0]=0;
				value_data[2*idx+0] = value_r[idx];
			}
		}
		else
		{
			is_null[3*idx+0]=1;
			value_data[2*idx+0]=(Type)0;	//useless
		}

		if(idx < value_w.size() && !isNull)
		{
			if(is_nan_or_inf(value_w[idx]))
			{
				is_null[3*idx+1]=1;
				value_data[2*idx+1]=(Type)0;	//useless
			}
			else
			{
				is_null[3*idx+1]=0;
				value_data[2*idx+1] = value_w[idx];
			}
		}
		else
		{
			is_null[3*idx+1]=1;
			value_data[2*idx+1]=(Type)0;	//useless
		}

		int_data[8*idx+0] = ID;
		if(detected_quality)
			int_data[8*idx+1] = quality;
		if(detected_error)
		{
			int_data[8*idx+2] = ERR_ID;
			if(ERR_ID < 0)
				is_null[3*idx+2]=1;
			else
				is_null[3*idx+2]=0;
		}
		int_data[8*idx+3] = idx;
		int_data[8*idx+4] = attr_r_dim.dim_x;
		int_data[8*idx+5] = attr_r_dim.dim_y;
		if(!(write_type == Tango::READ))
		{
			int_data[8*idx+6] = attr_w_dim.dim_x;
			int_data[8*idx+7] = attr_w_dim.dim_y;
		}
		double_data[2*idx+0] = ev_time;
		if(detected_recv_time)
			double_data[2*idx+1] = rcv_time;

		size_t plog_bind_ind = 0;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&double_data[2*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		if(detected_recv_time)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&double_data[2*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;
		}

		if(detected_quality)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;
		}

		if(detected_error)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+2];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+2];
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].is_unsigned= 1;
			plog_bind_ind++;
		}

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+3];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+4];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+5];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= mysql_value_type;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&value_data[2*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_unsigned= is_unsigned;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		if(!(write_type == Tango::READ))
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+6];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;

			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+7];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;

			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= mysql_value_type;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&value_data[2*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_unsigned= is_unsigned;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;
		}
	}

	if(max_size == 0)
	{
		int idx = 0;

		is_null[3*idx+0]=1;
		value_data[2*idx+0]=(Type)0;	//useless
		is_null[3*idx+1]=1;
		value_data[2*idx+1]=(Type)0;	//useless

		int_data[8*idx+0] = ID;
		if(detected_quality)
			int_data[8*idx+1] = quality;
		if(detected_error)
			int_data[8*idx+2] = ERR_ID;
		if(ERR_ID < 0)
			is_null[3*idx+2]=1;
		else
			is_null[3*idx+2]=0;
		int_data[8*idx+3] = idx;
		int_data[8*idx+4] = attr_r_dim.dim_x;
		int_data[8*idx+5] = attr_r_dim.dim_y;
		if(!(write_type == Tango::READ))
		{
			int_data[8*idx+6] = attr_w_dim.dim_x;
			int_data[8*idx+7] = attr_w_dim.dim_y;
		}
		double_data[2*idx+0] = ev_time;
		if(detected_recv_time)
			double_data[2*idx+1] = rcv_time;

		size_t plog_bind_ind = 0;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&double_data[2*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		if(detected_recv_time)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&double_data[2*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;
		}

		if(detected_quality)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;
		}

		if(detected_error)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+2];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+2];
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].is_unsigned= 1;
			plog_bind_ind++;
		}

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+3];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+4];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+5];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= mysql_value_type;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&value_data[2*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		if(!(write_type == Tango::READ))
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+6];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;

			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+7];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;

			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= mysql_value_type;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&value_data[2*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;
		}
	}

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		stringstream tmp;
		tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	if (mysql_stmt_execute(pstmt))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str();
		cout << __func__<< ": " << tmp.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt) << endl;
		delete [] plog_bind;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
		stringstream tmp;
		tmp << "failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		delete [] plog_bind;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
	delete [] plog_bind;
	return 0;
}

//=============================================================================
//=============================================================================
int HdbPPMySQL::store_scalar_string(string attr, vector<string> value_r, vector<string> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, double ev_time, double rcv_time, string table_name, bool isNull)
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
			Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found for attr="<<attr << endl;
		Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
	}
	int ID=it->second;

	bool detected_insert_time = true;
	bool detected_recv_time = true;
	bool detected_quality = true;
	bool detected_error = true;
	if(autodetectschema)
	{
		try
		{
			detected_insert_time = table_column_map.at(get_table_name(data_type, Tango::SCALAR, write_type)+"_"+SC_COL_INS_TIME);
		}
		catch(std::out_of_range &e)
		{
			detected_insert_time = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...SC_COL_INS_TIME) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_recv_time = table_column_map.at(get_table_name(data_type, Tango::SCALAR, write_type)+"_"+SC_COL_RCV_TIME);
		}
		catch(std::out_of_range &e)
		{
			detected_recv_time = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...SC_COL_RCV_TIME) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_quality = table_column_map.at(get_table_name(data_type, Tango::SCALAR, write_type)+"_"+SC_COL_QUALITY);
		}
		catch(std::out_of_range &e)
		{
			detected_quality = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...SC_COL_QUALITY) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_error = table_column_map.at(get_table_name(data_type, Tango::SCALAR, write_type)+"_"+SC_COL_ERROR_DESC_ID);
		}
		catch(std::out_of_range &e)
		{
			detected_error = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...SC_COL_ERROR_DESC) NOT FOUND" << endl;
#endif
		}
	}
	else if(lightschema)
	{
		detected_insert_time = false;
		detected_recv_time = false;
	}
	int ERR_ID=-1;
	if(detected_error)
		cache_err_id(error_desc, ERR_ID);

	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << table_name <<
			" (" << SC_COL_ID << "," SC_COL_EV_TIME << ",";
	if(detected_insert_time)
		query_str << SC_COL_INS_TIME << ",";
	if(detected_recv_time)
		query_str << SC_COL_RCV_TIME << ",";
	if(detected_quality)
		query_str << SC_COL_QUALITY << ",";
	if(detected_error)
		query_str << SC_COL_ERROR_DESC_ID << ",";
	query_str << SC_COL_VALUE_R;
	if(!(write_type == Tango::READ))	//RW
		query_str << "," << SC_COL_VALUE_W;
	query_str << ")";

	query_str << " VALUES (?,FROM_UNIXTIME(?),";
	if(detected_insert_time)
		query_str << "NOW(6),";
	if(detected_recv_time)
		query_str << "FROM_UNIXTIME(?),";
	if(detected_quality)
		query_str << "?,";
	if(detected_error)
		query_str << "?,";
	query_str << "?";	//value_r
	if(!(write_type == Tango::READ))	//RW
		query_str << ",?";
	query_str << ")";

	MYSQL_STMT	*pstmt;
	MYSQL_BIND	plog_bind[7];
	my_bool		is_null[3];    /* value nullability */
	double		double_data[2];
	string		value_data[2];
	unsigned long value_data_len[2];
	int			int_data[3];
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		Tango::Except::throw_exception(QUERY_ERROR,"mysql_stmt_init(): out of memory",__func__);
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		stringstream tmp;
		tmp << "mysql_stmt_prepare(), INSERT failed" << ", err='" << mysql_stmt_error(pstmt) << "' query='" << query_str.str() << "'";
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
	if(detected_quality)
		int_data[1] = quality;
	double_data[0] = ev_time;
	if(detected_recv_time)
		double_data[1] = rcv_time;
	if(detected_error)
	{
		int_data[2] = ERR_ID;
		if(ERR_ID < 0)
			is_null[2]=1;
		else
			is_null[2]=0;
	}
	memset(plog_bind, 0, sizeof(plog_bind));

	size_t plog_bind_ind = 0;

	plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
	plog_bind[plog_bind_ind].buffer= (void *)&int_data[0];
	plog_bind[plog_bind_ind].is_null= 0;
	plog_bind[plog_bind_ind].length= 0;
	plog_bind_ind++;

	plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
	plog_bind[plog_bind_ind].buffer= (void *)&double_data[0];
	plog_bind[plog_bind_ind].is_null= 0;
	plog_bind[plog_bind_ind].length= 0;
	plog_bind_ind++;

	if(detected_recv_time)
	{
		plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[plog_bind_ind].buffer= (void *)&double_data[1];
		plog_bind[plog_bind_ind].is_null= 0;
		plog_bind[plog_bind_ind].length= 0;
		plog_bind_ind++;
	}

	if(detected_quality)
	{
		plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[plog_bind_ind].buffer= (void *)&int_data[1];
		plog_bind[plog_bind_ind].is_null= 0;
		plog_bind[plog_bind_ind].length= 0;
		plog_bind_ind++;
	}

	if(detected_error)
	{
		plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[plog_bind_ind].buffer= (void *)&int_data[2];
		plog_bind[plog_bind_ind].is_null= &is_null[2];
		plog_bind[plog_bind_ind].length= 0;
		plog_bind[plog_bind_ind].is_unsigned= 1;
		plog_bind_ind++;
	}

	plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[plog_bind_ind].buffer= (void *)value_data[0].c_str();
	plog_bind[plog_bind_ind].is_null= &is_null[0];
	value_data_len[0]=value_data[0].length();
	plog_bind[plog_bind_ind].length= &value_data_len[0];
	plog_bind_ind++;

	plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_VARCHAR;
	plog_bind[plog_bind_ind].buffer= (void *)value_data[1].c_str();
	plog_bind[plog_bind_ind].is_null= &is_null[1];
	value_data_len[1]=value_data[1].length();
	plog_bind[plog_bind_ind].length= &value_data_len[1];
	plog_bind_ind++;

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		stringstream tmp;
		tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	if (mysql_stmt_execute(pstmt))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
		stringstream tmp;
		tmp << "failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	return 0;
}

//=============================================================================
//=============================================================================
int HdbPPMySQL::store_array_string(string attr, vector<string> value_r, vector<string> value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, string error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, string table_name, bool isNull)
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
			Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found from attr="<<attr << endl;
		Tango::Except::throw_exception(DATA_ERROR,"ID not found",__func__);
	}
	int ID=it->second;
	uint32_t max_size = (value_r.size() > value_w.size()) ? value_r.size() : value_w.size();

	bool detected_insert_time = true;
	bool detected_recv_time = true;
	bool detected_quality = true;
	bool detected_error = true;
	if(autodetectschema)
	{
		try
		{
			detected_insert_time = table_column_map.at(get_table_name(data_type, Tango::SPECTRUM, write_type)+"_"+ARR_COL_INS_TIME);
		}
		catch(std::out_of_range &e)
		{
			detected_insert_time = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...ARR_COL_INS_TIME) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_recv_time = table_column_map.at(get_table_name(data_type, Tango::SPECTRUM, write_type)+"_"+ARR_COL_RCV_TIME);
		}
		catch(std::out_of_range &e)
		{
			detected_recv_time = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...ARR_COL_RCV_TIME) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_quality = table_column_map.at(get_table_name(data_type, Tango::SPECTRUM, write_type)+"_"+ARR_COL_QUALITY);
		}
		catch(std::out_of_range &e)
		{
			detected_quality = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...ARR_COL_QUALITY) NOT FOUND" << endl;
#endif
		}
		try
		{
			detected_error = table_column_map.at(get_table_name(data_type, Tango::SPECTRUM, write_type)+"_"+ARR_COL_ERROR_DESC_ID);
		}
		catch(std::out_of_range &e)
		{
			detected_error = false;
#ifdef _LIB_DEBUG
			cout << __func__<< ": after table_column_map.at(...ARR_COL_ERROR_DESC) NOT FOUND" << endl;
#endif
		}
	}
	else if(lightschema)
	{
		detected_insert_time = false;
		detected_recv_time = false;
	}
	int ERR_ID=-1;
	if(detected_error)
		cache_err_id(error_desc, ERR_ID);

	ostringstream query_str;

	query_str <<
		"INSERT INTO " << m_dbname << "." << table_name <<
			" (" << ARR_COL_ID << "," << ARR_COL_EV_TIME << ",";
	if(detected_insert_time)
		query_str << ARR_COL_INS_TIME << ",";
	if(detected_recv_time)
		query_str << ARR_COL_RCV_TIME << ",";
	if(detected_quality)
		query_str << ARR_COL_QUALITY << ",";
	if(detected_error)
		query_str << ARR_COL_ERROR_DESC_ID << ",";
	query_str << ARR_COL_IDX << "," << ARR_COL_DIMX_R << "," <<	ARR_COL_DIMY_R << "," << ARR_COL_VALUE_R;
	if(!(write_type == Tango::READ))	//RW
		query_str << "," << ARR_COL_DIMX_W << "," << ARR_COL_DIMY_W << "," << ARR_COL_VALUE_W;
	query_str << ")";

	query_str << " VALUES ";
	for(unsigned int idx=0; idx < max_size; idx++)
	{
		query_str << "(?,FROM_UNIXTIME(?),";
		if(detected_insert_time)
			query_str << "NOW(6),";
		if(detected_recv_time)
			query_str << "FROM_UNIXTIME(?),";
		if(detected_quality)
			query_str << "?,";
		if(detected_error)
			query_str << "?,";
		query_str << "?,?,?,?";
		if(!(write_type == Tango::READ))	//RW
			query_str << ",?,?,?";
		query_str << ")";
		if(idx < max_size-1)
			query_str << ",";
	}
	int param_count_single = 6;	//param in single value insert
	if(detected_recv_time)
		param_count_single++;
	if(detected_quality)
		param_count_single++;
	if(detected_error)
		param_count_single++;
	if(write_type != Tango::READ)
		param_count_single += 3;
	int param_count = param_count_single*max_size;								//total param
	MYSQL_STMT	*pstmt;
	MYSQL_BIND	*plog_bind = new MYSQL_BIND[param_count];
	my_bool		is_null[3*max_size];    /* value nullability */	//value_r, value_w
	double		double_data[2*max_size];	// rcv_time, ev_time
	string		value_data[2*max_size];		//value_r, value_w
	unsigned long value_data_len[2*max_size];
	int			int_data[8*max_size];		//id, quality, error_desc_id, idx, dimx_r, dimy_r, dimx_w, dimy_w
	pstmt = mysql_stmt_init(dbp);
	if (!pstmt)
	{
		cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		Tango::Except::throw_exception(QUERY_ERROR,"mysql_stmt_init(): out of memory",__func__);
	}
	if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
	{
		stringstream tmp;
		tmp << "mysql_stmt_prepare(), prepare stmt failed, stmt='"<<query_str.str()<<"' err="<<mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
	memset(plog_bind, 0, sizeof(MYSQL_BIND)*param_count);

	for(size_t idx=0; idx < max_size; idx++)
	{
		if(idx < value_r.size() && !isNull)
		{
			is_null[3*idx+0]=0;
			value_data[2*idx+0] = value_r[idx];
		}
		else
		{
			is_null[3*idx+0]=1;
			value_data[2*idx+0]="";	//useless
		}

		if(idx < value_w.size() && !isNull)
		{
			is_null[3*idx+1]=0;
			value_data[2*idx+1] = value_w[idx];
		}
		else
		{
			is_null[3*idx+1]=1;
			value_data[2*idx+1]="";	//useless
		}

		int_data[8*idx+0] = ID;
		if(detected_quality)
			int_data[8*idx+1] = quality;
		if(detected_error)
		{
			int_data[8*idx+2] = ERR_ID;
			if(ERR_ID < 0)
				is_null[3*idx+2]=1;
			else
				is_null[3*idx+2]=0;
		}
		int_data[8*idx+3] = idx;
		int_data[8*idx+4] = attr_r_dim.dim_x;
		int_data[8*idx+5] = attr_r_dim.dim_y;
		if(!(write_type == Tango::READ))
		{
			int_data[8*idx+6] = attr_w_dim.dim_x;
			int_data[8*idx+7] = attr_w_dim.dim_y;
		}
		double_data[2*idx+0] = ev_time;
		if(detected_recv_time)
			double_data[2*idx+1] = rcv_time;

		size_t plog_bind_ind = 0;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&double_data[2*idx+0];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		if(detected_recv_time)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&double_data[2*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;
		}

		if(detected_quality)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+1];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;
		}

		if(detected_error)
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+2];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+2];
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].is_unsigned= 1;
			plog_bind_ind++;
		}

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+3];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+4];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+5];
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
		plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
		plog_bind_ind++;

		plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_VARCHAR;
		plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)value_data[2*idx+0].c_str();
		plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+0];
		value_data_len[2*idx+0]=value_data[2*idx+0].length();
		plog_bind[param_count_single*idx+plog_bind_ind].length= &value_data_len[2*idx+0];
		plog_bind_ind++;

		if(!(write_type == Tango::READ))
		{
			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+6];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;

			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)&int_data[8*idx+7];
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= 0;
			plog_bind[param_count_single*idx+plog_bind_ind].length= 0;
			plog_bind_ind++;

			plog_bind[param_count_single*idx+plog_bind_ind].buffer_type= MYSQL_TYPE_VARCHAR;
			plog_bind[param_count_single*idx+plog_bind_ind].buffer= (void *)value_data[2*idx+1].c_str();
			plog_bind[param_count_single*idx+plog_bind_ind].is_null= &is_null[3*idx+1];
			value_data_len[2*idx+1]=value_data[2*idx+1].length();
			plog_bind[param_count_single*idx+plog_bind_ind].length= &value_data_len[2*idx+1];
			plog_bind_ind++;
		}
	}

	if (mysql_stmt_bind_param(pstmt, plog_bind))
	{
		stringstream tmp;
		tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}

	if (mysql_stmt_execute(pstmt))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt) << endl;
		delete [] plog_bind;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
		stringstream tmp;
		tmp << "failed while closing the statement" << ", err=" << mysql_stmt_error(pstmt);
		cout << __func__<< ": " << tmp.str() << endl;
		delete [] plog_bind;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
	delete [] plog_bind;
	return 0;
}

template <typename Type> bool HdbPPMySQL::is_nan_or_inf(Type val)
{
	return false;
}
template <> bool HdbPPMySQL::is_nan_or_inf(double val)
{
	return std::isnan(val) || std::isinf(val);
}
template <> bool HdbPPMySQL::is_nan_or_inf(float val)
{
	return std::isnan(val) || std::isinf(val);
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
#endif
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

void HdbPPMySQL::string_vector2map(vector<string> str, string separator, map<string,string>* results)
{
	for(vector<string>::iterator it=str.begin(); it != str.end(); it++)
	{
		string::size_type found_eq;
		found_eq = it->find_first_of(separator);
		if(found_eq != string::npos && found_eq > 0)
			results->insert(make_pair(it->substr(0,found_eq),it->substr(found_eq+1)));
	}
}

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

	if(type==Tango::DEV_DOUBLE)
	{
		data_type << TYPE_DEV_DOUBLE << "_";
	}
	else if(type==Tango::DEV_FLOAT)
	{
		data_type << TYPE_DEV_FLOAT << "_";
	}
	else if(type==Tango::DEV_STRING)	
	{
		data_type << TYPE_DEV_STRING << "_";
	}
	else if(type==Tango::DEV_LONG)
	{
		data_type << TYPE_DEV_LONG << "_";
	}
	else if(type==Tango::DEV_ULONG)
	{
		data_type << TYPE_DEV_ULONG << "_";
	}
	else if(type==Tango::DEV_LONG64)
	{
		data_type << TYPE_DEV_LONG64 << "_";
	}
	else if(type==Tango::DEV_ULONG64)
	{
		data_type << TYPE_DEV_ULONG64 << "_";
	}
	else if(type==Tango::DEV_SHORT)
	{
		data_type << TYPE_DEV_SHORT << "_";
	}
	else if(type==Tango::DEV_USHORT)
	{
		data_type << TYPE_DEV_USHORT << "_";
	}
	else if(type==Tango::DEV_BOOLEAN)
	{
		data_type << TYPE_DEV_BOOLEAN << "_";
	}
	else if(type==Tango::DEV_UCHAR)
	{
		data_type << TYPE_DEV_UCHAR << "_";
	}
	else if(type==Tango::DEV_STATE)
	{
		data_type << TYPE_DEV_STATE << "_";
	}
	else if(type==Tango::DEV_ENCODED)
	{
		data_type << TYPE_DEV_ENCODED << "_";
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
bool HdbPPMySQL::autodetect_column(int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/, string column_name)
{
	ostringstream query_str;
	query_str <<
		"SELECT " << INF_SCHEMA_COLUMN_NAME << " FROM " << INFORMATION_SCHEMA << "." << INF_SCHEMA_COLUMNS << " WHERE " << INF_SCHEMA_TABLE_SCHEMA <<
			"='" << m_dbname << "' AND " << INF_SCHEMA_TABLE_NAME << "='" << get_table_name(type, format, write_type) << "'";
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		//Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
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
			return false;
		}
#ifdef _LIB_DEBUG
		else
		{
			my_ulonglong num_found = mysql_num_rows(res);
			if(num_found > 0)
			{
				cout << __func__<< ": SUCCESS in query: " << query_str.str() << " num_found=" << num_found << endl;
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
			//cout << __func__<< ": looping result -> " << row[0] << " looking for -> " << column_name << endl;
			if(row[0] == column_name)
			{
				found = true;
				break;
			}
		}
		mysql_free_result(res);
		return found;
	}
	return false;
}

//=============================================================================
//=============================================================================
AbstractDB* HdbPPMySQLFactory::create_db(vector<string> configuration)
{
	return new HdbPPMySQL(configuration);
}

//=============================================================================
//=============================================================================
DBFactory *HdbClient::getDBFactory()
{
	HdbPPMySQLFactory *db_mysql_factory = new HdbPPMySQLFactory();
	return static_cast<DBFactory*>(db_mysql_factory);//TODO
}

