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
#include <errmsg.h> //mysql error codes

#define MYSQL_ERROR		"Mysql Error"
#define CONFIG_ERROR	"Configuration Error"
#define QUERY_ERROR		"Query Error"
#define DATA_ERROR		"Data Error"

#define RETRY_QUERY_CNT	1

#ifndef LIB_BUILDTIME
#define LIB_BUILDTIME   RELEASE " " __DATE__ " "  __TIME__
#endif

const char version_string[] = "$Build: " LIB_BUILDTIME " $";
static const char __FILE__rev[] = __FILE__ " $Id: $";

#define ERR_MAP_MAX_SIZE				50000

//#define _LIB_DEBUG

namespace hdbpp
{
struct dt_hashing_func
{
  std::size_t operator()(const HdbEventDataType& k) const
  {
    using std::size_t;
    using std::hash;
    using std::string;

    return ((hash<int>()(k.data_type)
             ^ (hash<int>()((int)k.data_format) << 1)) >> 1)
             ^ (hash<int>()(k.write_type) << 1);
  }
};

struct dt_equal_func {
    public:
        bool operator()(const HdbEventDataType& t1, const HdbEventDataType& t2) const {
            return (t1.data_type == t2.data_type && t1.data_format == t2.data_format && t1.write_type == t2.write_type);
        }
};

template <typename T> T foo_value()
{
	return static_cast<T>(0);
}

template <> string foo_value()
{
	return string("");
}

template <typename T> string to_json_value(T val)
{
	stringstream ss;
	ss << std::scientific << std::setprecision(std::numeric_limits<T>::digits10+2) << val;
	return ss.str();
}

template <> string to_json_value(Tango::DevUChar val)
{
	stringstream ss;
	ss << static_cast<int>(val);
	return ss.str();
}

template <> string to_json_value(string val)
{
	stringstream ss;
	ss << "\"" << val << "\"";
	return ss.str();
}

template <typename T> void bind_value(MYSQL_BIND &log_bind, enum_field_types mysql_value_type , const T	&value_data, unsigned long &value_data_len, my_bool &is_null, bool is_unsigned)
{
	log_bind.buffer_type= mysql_value_type;
	log_bind.buffer= (void *)&value_data;
	log_bind.is_null= &is_null;
	log_bind.is_unsigned= is_unsigned;
	log_bind.length= 0;
}

template <> void bind_value(MYSQL_BIND &log_bind, enum_field_types mysql_value_type , const string &value_data, unsigned long &value_data_len, my_bool &is_null, bool is_unsigned)
{
	log_bind.buffer_type= mysql_value_type; //MYSQL_TYPE_VARCHAR
	log_bind.buffer= (void *)value_data.c_str();
	log_bind.is_null= &is_null;
	value_data_len=value_data.length();
	log_bind.length= &value_data_len;
}

struct HdbppStringUtils
{
	static void string_explode(string str, const string& separator, vector<string>& results);
	static void string_vector2map(const vector<string> &str, const string &separator, unordered_map<string,string> &results);
	static string get_only_attr_name(const string& str);
	static string get_only_tango_host(const string& str);
	static string add_domain(const string& facility);
};

void HdbppStringUtils::string_explode(string str, const string& separator, vector<string>& results)
{
	auto found = str.find_first_of(separator);
	while(found != string::npos) {
		if(found > 0) {
			results.push_back(str.substr(0,found));
		}
		str = str.substr(found+1);
		found = str.find_first_of(separator);
	}
	if(str.length() > 0) {
		results.push_back(str);
	}
}

void HdbppStringUtils::string_vector2map(const vector<string> &str, const string &separator, unordered_map<string,string> &results)
{
	for(const auto &it : str)
	{
		auto found_eq = it.find_first_of(separator);
		if(found_eq != string::npos && found_eq > 0)
			results.insert(make_pair(it.substr(0,found_eq),it.substr(found_eq+1)));
	}
}

string HdbppStringUtils::get_only_attr_name(const string& str)
{
	auto start = str.find("tango://");
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
string HdbppStringUtils::get_only_tango_host(const string& str)
{
	auto start = str.find("tango://");
	if (start == string::npos)
	{
		return "unknown";
	}
	else
	{
		start += 8; //	"tango://" length
		auto end = str.find('/', start);
		string th = str.substr(start, end-start);
		return th;
	}
}
//=============================================================================
//=============================================================================
string HdbppStringUtils::add_domain(const string& str)
{
	auto end1 = str.find(".");
	if (end1 == string::npos)
	{
		//get host name without tango://
		auto start = str.find("tango://");
		if (start == string::npos)
		{
			start = 0;
		}
		else
		{
			start = 8;	//tango:// len
		}
		auto end2 = str.find(":", start);

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

		for(rp = result; rp != NULL; rp = rp->ai_next)
		{
			with_domain = string(rp->ai_canonname) + str.substr(end2);
			//cout << __func__ <<": found domain -> " << with_domain<<endl;
		}
		freeaddrinfo(result); // all done with this structure
		return with_domain;
	}
	else
	{
		return str;
	}
}


HdbPPMySQL::HdbPPMySQL(const string &id, const vector<string> &configuration)
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
	ignoreduplicates = false;
	jsonarray = false;
	dbp = new MYSQL();
	if(!mysql_init(dbp))
	{
		stringstream tmp;
		cout << __func__<<": VERSION: " << version_string << " file:" << __FILE__rev << endl;
		tmp << "mysql init db error: " << mysql_errno(dbp) << " - " << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(MYSQL_ERROR,tmp.str(),__func__);
	}
	my_bool my_auto_reconnect=1;
	if(mysql_options(dbp,MYSQL_OPT_RECONNECT,&my_auto_reconnect) !=0)
	{
		cout << __func__<<": mysql auto reconnection error: " << mysql_errno(dbp) << " - " << mysql_error(dbp) << endl;
	}

	unordered_map<string,string> db_conf;
	HdbppStringUtils::string_vector2map(configuration,"=",db_conf);
	string host, user, password, dbname;
	int port;
	try
	{
		host = db_conf.at("host");
		user = db_conf.at("user");
		password = db_conf.at("password");
		dbname = db_conf.at("dbname");
		m_dbname = dbname;
		port = stoi(db_conf.at("port"));
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
		ilightschema = stoi(db_conf.at("lightschema"));
		lightschema = (ilightschema == 1);
	}
	catch(const std::out_of_range& e)
	{
#ifdef _LIB_DEBUG
		//cout << __func__<< ": lightschema key not found" << endl;
#endif
		autodetectschema = true;
	}
	try
	{
		int iignoreduplicates;
		iignoreduplicates = stoi(db_conf.at("ignore_duplicates"));
		ignoreduplicates = (iignoreduplicates == 1);
	}
	catch(const std::out_of_range& e)
	{
#ifdef _LIB_DEBUG
		cout << __func__<< ": ignore_duplicates key not found" << endl;
#endif
		ignoreduplicates = false;
	}
	try
	{
		int ijsonarray;
		ijsonarray = stoi(db_conf.at("json_array"));
		jsonarray = (ijsonarray == 1);
		cout << __func__<< ": json_array key FOUND val="<< ijsonarray<< endl;
	}
	catch(const std::out_of_range& e)
	{
//#ifdef _LIB_DEBUG
		cout << __func__<< ": json_array key not found" << endl;
//#endif
		jsonarray = false;	//default to arrays with one row per array datum
	}
	try
	{
		batch_size = stoi(db_conf.at("batch_size"));
		cout << __func__<< ": batch_size key FOUND val="<< batch_size<< endl;
	}
	catch(const std::out_of_range& e)
	{
//#ifdef _LIB_DEBUG
		cout << __func__<< ": batch_size key not found, using " << batch_size << endl;
//#endif
	}
	if(!mysql_real_connect(dbp, host.c_str(), user.c_str(), password.c_str(), dbname.c_str(), port, NULL, 0))
	{
		stringstream tmp;
		tmp << "mysql connect db error: " << mysql_errno(dbp) << " - " << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(MYSQL_ERROR,tmp.str(),__func__);
	}
	else
	{
		db_mti = mysql_thread_id(dbp);
		//everything OK
#ifdef _LIB_DEBUG
		cout << __func__<< ": mysql connection OK, mysql_thread_id=" << db_mti << endl;
#endif
	}
	if(autodetectschema)
	{
		for(const auto& it_type : v_type)
		{
			for(const auto& it_format : v_format)
			{
				for(const auto& it_write_type : v_write_type)
				{
					string table_name = get_table_name(it_type, it_format, it_write_type);
					if(it_format == Tango::SCALAR)
					{
						bool detected=autodetect_column(table_name, SC_COL_INS_TIME);
						table_column_map.insert(make_pair(table_name+"_"+SC_COL_INS_TIME, detected));

						detected=autodetect_column(table_name, SC_COL_RCV_TIME);
						table_column_map.insert(make_pair(table_name+"_"+SC_COL_RCV_TIME, detected));

						detected=autodetect_column(table_name, SC_COL_QUALITY);
						table_column_map.insert(make_pair(table_name+"_"+SC_COL_QUALITY, detected));

						detected=autodetect_column(table_name, SC_COL_ERROR_DESC_ID);
						table_column_map.insert(make_pair(table_name+"_"+SC_COL_ERROR_DESC_ID, detected));
					}
					else
					{
						bool detected=autodetect_column(table_name, ARR_COL_INS_TIME);
						table_column_map.insert(make_pair(table_name+"_"+ARR_COL_INS_TIME, detected));

						detected=autodetect_column(table_name, ARR_COL_RCV_TIME);
						table_column_map.insert(make_pair(table_name+"_"+ARR_COL_RCV_TIME, detected));

						detected=autodetect_column(table_name, ARR_COL_QUALITY);
						table_column_map.insert(make_pair(table_name+"_"+ARR_COL_QUALITY, detected));

						detected=autodetect_column(table_name, ARR_COL_ERROR_DESC_ID);
						table_column_map.insert(make_pair(table_name+"_"+ARR_COL_ERROR_DESC_ID, detected));
					}
				}
			}
		}
		bool detected=autodetect_column(PARAM_TABLE_NAME, PARAM_COL_INS_TIME);
		table_column_map.insert(make_pair(string(PARAM_TABLE_NAME)+"_"+PARAM_COL_INS_TIME, detected));
	}
}

HdbPPMySQL::~HdbPPMySQL()
{
	for(auto it_pstmt : pstmt_map)
	{
		if (mysql_stmt_close(it_pstmt.second))
		{
			stringstream tmp;
			tmp << "failed while closing the statement" << ", err=" << mysql_error(dbp);
			cout << __func__<< ": " << tmp.str() << endl;
			//Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
		}
	}
	mysql_close(dbp);
	delete dbp;
}

int HdbPPMySQL::find_attr_id(string facility, string attr, int &ID)
{
	ostringstream query_str;
	query_str << 
		"SELECT " << CONF_COL_ID << " FROM " << m_dbname << "." << CONF_TABLE_NAME <<
			" WHERE " << CONF_COL_NAME << " = 'tango://" << facility<<"/"<<attr << "'";
	mysql_ping(dbp); // to trigger reconnection if disconnected
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
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
	string db_type;
	query_str << 
		"SELECT " << CONF_TABLE_NAME << "." << CONF_COL_ID << "," << CONF_TYPE_TABLE_NAME << "." << CONF_TYPE_COL_TYPE << "," << CONF_TABLE_NAME << "." << CONF_COL_TTL <<
			" FROM " << m_dbname << "." << CONF_TABLE_NAME <<
			" JOIN " << m_dbname << "." << CONF_TYPE_TABLE_NAME <<
			" ON " << m_dbname << "." << CONF_TABLE_NAME << "." << CONF_COL_TYPE_ID << "=" << m_dbname << "." << CONF_TYPE_TABLE_NAME << "." << CONF_TYPE_COL_TYPE_ID <<
			" WHERE " << CONF_COL_NAME << " = 'tango://" << facility<<"/"<<attr << "'";
	mysql_ping(dbp); // to trigger reconnection if disconnected
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
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

	query_str <<
		"SELECT " << HISTORY_EVENT_COL_EVENT <<
			" FROM " << m_dbname << "." << HISTORY_TABLE_NAME <<
			" JOIN " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME <<
			" ON " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME << "." << HISTORY_EVENT_COL_EVENT_ID << "=" << m_dbname << "." << HISTORY_TABLE_NAME << "." << HISTORY_COL_EVENT_ID <<
			" WHERE " << HISTORY_COL_ID << " = " << ID <<
			" ORDER BY " << HISTORY_COL_TIME << " DESC LIMIT 1";

	mysql_ping(dbp); // to trigger reconnection if disconnected
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
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
	query_str <<
		"SELECT " << ERR_COL_ID << " FROM " << m_dbname << "." << ERR_TABLE_NAME <<
			" WHERE " << ERR_COL_ERROR_DESC << " = '" << err_escaped << "'";

	if(mysql_query(dbp, query_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
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
	auto it = attr_ERR_ID_map.find(error_desc);
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

	MYSQL_STMT	*pstmt{nullptr};
	MYSQL_BIND	plog_bind[1];
	string		param_data[1];
	unsigned long param_data_len[1];

	bool cached = cache_pstmt(query_str.str(),&pstmt,1,__func__);

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
		err_stmt_close(cached, __func__, query_str.str(), pstmt);
	}
	else
	{
		stmt_close(cached, __func__, pstmt);
#ifdef _LIB_DEBUG
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
		ostringstream query_last_str;
		query_last_str <<
			"SELECT LAST_INSERT_ID() FROM " << m_dbname << "." << ERR_TABLE_NAME;

		if(mysql_query(dbp, query_last_str.str().c_str()))
		{
			stringstream tmp;
			tmp << "ERROR in query=" << query_last_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
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
	return 0;
}

void HdbPPMySQL::insert_event(Tango::EventData *data, const HdbEventDataType &ev_data_type)
{
#ifdef _LIB_DEBUG
	//cout << __func__<< ": entering..." << endl;
#endif
	prepare_insert_event(std::vector<Tango::EventData *>{data}, ev_data_type);
#ifdef _LIB_DEBUG
	//cout << __func__<< ": exiting... =" << endl;
#endif
}

void HdbPPMySQL::insert_events(vector<tuple<Tango::EventData *, HdbEventDataType>> events)
{
#ifdef _LIB_DEBUG
	//cout << __func__<< ": entering..." << endl;
#endif
	mysql_autocommit(dbp, 0);
	unordered_map<HdbEventDataType, vector<Tango::EventData *>, dt_hashing_func, dt_equal_func > data;
	try
	{
		for(const auto& event : events)
		{
			data[get<1>(event)].push_back(std::move(get<0>(event)));
		}
		for(const auto& event_dt : data)
		{
			prepare_insert_event(event_dt.second, event_dt.first);
		}
	}
	catch (Tango::DevFailed &e)
	{
		mysql_rollback(dbp);
		mysql_autocommit(dbp, 1);
		throw;
	}
	mysql_autocommit(dbp, 1);
	mysql_commit(dbp); //TODO: NEEDED?
#ifdef _LIB_DEBUG
	//cout << __func__<< ": exiting... "<< endl;
#endif
}

template <typename Type> void HdbPPMySQL::extract_and_store(const vector<event_data_param> &event_data, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, Tango::AttrDataFormat data_format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/, enum_field_types mysql_value_type, bool _is_unsigned)
{
	string table_name = get_table_name(data_type, data_format, write_type);
#ifdef _LIB_DEBUG
	cout << __func__<< ": entering.. " << table_name << " event_data size="<<event_data.size() << endl;
#endif
	vector<event_values_param<Type> > values;
	values.reserve(event_data.size());
	for(const auto& event_datum : event_data)
	{
		event_values_param<Type> value;
		value.param = event_datum.param;
		if(!(!event_datum.param.isNull && ((write_type == Tango::READ && event_datum.data->attr_value->extract_read(value.val_r)) ||
			(write_type != Tango::READ && event_datum.data->attr_value->extract_read(value.val_r) && event_datum.data->attr_value->extract_set(value.val_w)))))
		{
			value.val_r.push_back(foo_value<Type>()); //fake value
			if(!event_datum.param.isNull)
				cout << __func__<<": failed to extract " << event_datum.param.attr_name << endl;
			value.param.isNull = true;
		}
#ifdef _LIB_DEBUG
		cout << __func__<< ": extracted .. value.val_r "<<value.val_r.size() << " and value.val_w " << value.val_w.size() << " values" << endl; 
#endif
		values.push_back(value);
	}
	if(data_format == Tango::SCALAR)
	{
		store_scalar<Type>(values, data_type, write_type, table_name, mysql_value_type, _is_unsigned);
	}
	else
	{
		if(jsonarray)
			store_array_json<Type>(values, data_type, write_type, table_name, mysql_value_type, _is_unsigned);
		else
			store_arrays<Type>(values, data_type, write_type, table_name, mysql_value_type, _is_unsigned);
	}
}

template<>  void HdbPPMySQL::extract_and_store<string>(const vector<event_data_param> &event_data, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, Tango::AttrDataFormat data_format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/, enum_field_types mysql_value_type, bool _is_unsigned)
{
	string table_name = get_table_name(data_type, data_format, write_type);
	vector<string>	val_r;
	vector<event_values_param<string> > values;
	values.reserve(event_data.size());
	for(const auto& event_datum : event_data)
	{
		event_values_param<string> value;
		value.param = event_datum.param;
		try
		{
			if(!(!event_datum.param.isNull && ((write_type == Tango::READ && event_datum.data->attr_value->extract_read(value.val_r)) ||
				(write_type != Tango::READ && event_datum.data->attr_value->extract_read(value.val_r) && event_datum.data->attr_value->extract_set(value.val_w)))))
			{
				value.val_r.push_back(foo_value<string>()); //fake value
				if(!event_datum.param.isNull)
					cout << __func__<<": failed to extract " << event_datum.param.attr_name << endl;
				value.param.isNull = true;
			}
		}
		catch(CORBA::BAD_PARAM &e)
		{
			//TODO: re_throw as Tango exception?
				value.val_r.push_back(foo_value<string>()); //fake value
				if(!event_datum.param.isNull)
					cout << __func__<<": CORBA::BAD_PARAM " << event_datum.param.attr_name << endl;
				value.param.isNull = true;
		}
		values.push_back(value);
	}
	if(data_format == Tango::SCALAR)
	{
		//store_scalar_string<string>(values, data_type, write_type, table_name);
		store_scalar<string>(values, data_type, write_type, table_name, mysql_value_type, _is_unsigned);
	}
	else
	{
		if(jsonarray)
			//store_array_string_json<string>(values, data_type, write_type, table_name);
			store_array_json<string>(values, data_type, write_type, table_name, mysql_value_type, _is_unsigned);
		else
			//store_arrays_string<string>(values, data_type, write_type, table_name);
			store_arrays<string>(values, data_type, write_type, table_name, mysql_value_type, _is_unsigned);
	}
}

void HdbPPMySQL::prepare_insert_event(vector<Tango::EventData *> data, const HdbEventDataType &ev_data_type)
{
#ifdef _LIB_DEBUG
	cout << __func__<< ": entering... data size="<<data.size() << " data_type=" << ev_data_type.data_type << " data_format=" << (int)ev_data_type.data_format << endl;
#endif
	vector<string> attr_names;// = data->attr_name;
	vector<double>	ev_times;
	vector<double>	rcv_times;// = data->get_date().tv_sec + (double)data->get_date().tv_usec/1.0e6;
	vector<int> qualities;// = (int)data->attr_value->get_quality();
	vector<string> error_descs;
	vector<event_data_param> event_data;
	event_data.reserve(data.size());



	vector<Tango::AttributeDimension> attr_w_dims;
	vector<Tango::AttributeDimension> attr_r_dims;
	int data_type = ev_data_type.data_type; //data->attr_value->get_type()
	Tango::AttrDataFormat data_format = ev_data_type.data_format;
	int write_type = ev_data_type.write_type;
	//int max_dim_x = ev_data_type.max_dim_x;
	//int max_dim_y = ev_data_type.max_dim_y;

	vector<bool> isNulls;

	for(const auto& datum : data)
	{
		event_data_param event_datum;
		event_datum.param.rcv_time = datum->get_date().tv_sec + (double)datum->get_date().tv_usec/1.0e6;
		event_datum.param.quality = (int)datum->attr_value->get_quality();

		event_datum.param.attr_name = datum->attr_name;

		datum->attr_value->reset_exceptions(Tango::DeviceAttribute::isempty_flag); //disable is_empty exception
		if(datum->err || datum->attr_value->is_empty()/* || datum->attr_value->get_quality() == Tango::ATTR_INVALID */)
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": going to archive as NULL .. " << event_datum.param.attr_name << endl;
#endif
			event_datum.param.isNull = true;
			if(datum->err)
			{
				event_datum.param.error_desc = datum->errors[0].desc;
				event_datum.param.ev_time = event_datum.param.rcv_time;
			}
		}
		if(!datum->err)
			event_datum.param.ev_time = datum->attr_value->get_date().tv_sec + (double)datum->attr_value->get_date().tv_usec/1.0e6;
#ifdef _LIB_DEBUG
		//cout << __func__<< ": data_type="<<data_type<<" data_format="<<data_format<<" write_type="<<write_type << endl;
#endif
		if(!event_datum.param.isNull)
		{
			event_datum.param.attr_w_dim = datum->attr_value->get_w_dimension();
			event_datum.param.attr_r_dim = datum->attr_value->get_r_dimension();
		}
		else
		{
			event_datum.param.attr_r_dim.dim_x = 0;//max_dim_x;//TODO: OK?
			event_datum.param.attr_w_dim.dim_x = 0;//max_dim_x;//TODO: OK?
			event_datum.param.attr_r_dim.dim_y = 0;//max_dim_y;//TODO: OK?
			event_datum.param.attr_w_dim.dim_y = 0;//max_dim_y;//TODO: OK?
		}
		if(event_datum.param.ev_time < 1)
			event_datum.param.ev_time=1;
		if(event_datum.param.rcv_time < 1)
			event_datum.param.rcv_time=1;
		event_datum.data=datum;
		event_data.push_back(event_datum);
	}

	switch(data_type)
	{
		case Tango::DEV_DOUBLE:
		{
			extract_and_store<Tango::DevDouble>(event_data, data_type, data_format, write_type, MYSQL_TYPE_DOUBLE, false/*is_unsigned*/);
			break;
		}
		case Tango::DEV_FLOAT:
		{
			extract_and_store<Tango::DevFloat>(event_data, data_type, data_format, write_type, MYSQL_TYPE_FLOAT, false/*is_unsigned*/);
			break;
		}
		case Tango::DEV_LONG:
		{
			extract_and_store<Tango::DevLong>(event_data, data_type, data_format, write_type, MYSQL_TYPE_LONG, false/*is_unsigned*/);
			break;
		}
		case Tango::DEV_ULONG:
		{
			extract_and_store<Tango::DevULong>(event_data, data_type, data_format, write_type, MYSQL_TYPE_LONG, true/*is_unsigned*/);
			break;
		}
		case Tango::DEV_LONG64:
		{
			extract_and_store<Tango::DevLong64>(event_data, data_type, data_format, write_type, MYSQL_TYPE_LONGLONG, false/*is_unsigned*/);
			break;
		}
		case Tango::DEV_ULONG64:
		{
			extract_and_store<Tango::DevULong64>(event_data, data_type, data_format, write_type, MYSQL_TYPE_LONGLONG, true/*is_unsigned*/);
			break;
		}
		case Tango::DEV_SHORT:
		{
			extract_and_store<Tango::DevShort>(event_data, data_type, data_format, write_type, MYSQL_TYPE_SHORT, false/*is_unsigned*/);
			break;
		}
		case Tango::DEV_USHORT:
		{
			extract_and_store<Tango::DevUShort>(event_data, data_type, data_format, write_type, MYSQL_TYPE_SHORT, true/*is_unsigned*/);
			break;
		}
		case Tango::DEV_BOOLEAN:
		{
			extract_and_store<Tango::DevBoolean>(event_data, data_type, data_format, write_type, MYSQL_TYPE_TINY, true/*is_unsigned*/);
			break;
		}
		case Tango::DEV_UCHAR:
		{
			extract_and_store<Tango::DevUChar>(event_data, data_type, data_format, write_type, MYSQL_TYPE_TINY, true/*is_unsigned*/);
			break;
		}
		case Tango::DEV_STRING:
		{
			extract_and_store<string>(event_data, data_type, data_format, write_type, MYSQL_TYPE_VARCHAR, false/*is_unsigned*/);
			break;
		}
		case Tango::DEV_STATE: //TODO batch insertion!!!!!!!!!!!!!
		{
#if 1
			if(write_type == Tango::READ && data_format == Tango::SCALAR)
			{

				vector<event_values_param<Tango::DevState> > values;
				values.reserve(event_data.size());
				size_t ind=0;
				for(const auto& event_datum : event_data)
				{
					event_values_param<Tango::DevState> value;
					value.param = event_datum.param;
					// We cannot use the extract_read() method for the "State" attribute
					Tango::DevState	st;
					if(!isNulls.at(ind))
					{
						*event_datum.data->attr_value >> st;
					}
					else
					{
						st = (Tango::DevState)0; //fake value
					}
					value.val_r.push_back(st);
					values.push_back(value);
				}
				string table_name = get_table_name(data_type, data_format, write_type);
				store_scalar<Tango::DevState>(values, data_type, write_type, table_name, MYSQL_TYPE_TINY, true/*is_unsigned*/);
			}
			else
			{
				extract_and_store<Tango::DevState>(event_data, data_type, data_format, write_type, MYSQL_TYPE_TINY, true/*is_unsigned*/);
			}
#else	//TODO: extract_read fails on state attribute
			ret = extract_and_store<Tango::DevState>(event_data, data_type, data_format, write_type, MYSQL_TYPE_TINY, true/*is_unsigned*/);
#endif
			break;
		}
		case Tango::DEV_ENUM:
		{
			extract_and_store<Tango::DevEnum>(event_data, data_type, data_format, write_type, MYSQL_TYPE_SHORT, false/*is_unsigned*/);
			break;
		}
		default:
		{
			TangoSys_MemStream	os;
			os << "Attribute type " << data_type << " not supported";
			cout << __func__<<": " << os.str() << endl;
			Tango::Except::throw_exception(DATA_ERROR,os.str(),__func__);
		}
	}
}


void HdbPPMySQL::insert_param_event(Tango::AttrConfEventData *data, const HdbEventDataType &ev_data_type)
{
#ifdef _LIB_DEBUG
	//cout << __func__<< ": entering..." << endl;
#endif

	string attr = data->attr_name;
	double	ev_time = data->get_date().tv_sec + (double)data->get_date().tv_usec/1.0e6;
	string error_desc("");

	int ID=cache_ID(data->attr_name, __func__);
	ostringstream query_str;

	bool detected_insert_time = true;
	if(autodetectschema)
	{
		const auto it = table_column_map.find(string(PARAM_TABLE_NAME)+"_"+PARAM_COL_INS_TIME);
		if(it!=table_column_map.end())
			detected_insert_time = it->second;
		else
			detected_insert_time = false;
	}

	if(!ignoreduplicates)
		query_str << "INSERT INTO ";
	else
		query_str << "INSERT IGNORE INTO ";
	query_str << m_dbname << "." << PARAM_TABLE_NAME <<
			" (" << PARAM_COL_ID << ",";

	if(detected_insert_time)
		query_str << PARAM_COL_INS_TIME << ",";

	query_str << PARAM_COL_EV_TIME << "," <<
			PARAM_COL_LABEL << "," << PARAM_COL_UNIT << "," << PARAM_COL_STANDARDUNIT << "," <<
			PARAM_COL_DISPLAYUNIT << "," << PARAM_COL_FORMAT << "," << PARAM_COL_ARCHIVERELCHANGE << "," <<
			PARAM_COL_ARCHIVEABSCHANGE << "," << PARAM_COL_ARCHIVEPERIOD << "," << PARAM_COL_DESCRIPTION << ")";

	query_str << " VALUES (?,";

	if(detected_insert_time)
		query_str << "NOW(6),";

	query_str << "FROM_UNIXTIME(?)," <<
			"?,?,?," <<
			"?,?,?," <<
			"?,?,?)" ;
	unsigned int retry_cnt=0;
	do
	{
		retry_cnt++;
		MYSQL_STMT	*pstmt{nullptr};
		MYSQL_BIND	plog_bind[11];
		double		double_data;
		int			int_data;
		string		param_data[9];
		unsigned long param_data_len[9];
		bool cached = cache_pstmt(query_str.str(),&pstmt,1,__func__);
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
			unsigned int mse = mysql_stmt_errno(pstmt);
			string error = err_stmt_close(cached, __func__, query_str.str(), pstmt);
			if((mse == CR_SERVER_LOST || mse == CR_SERVER_GONE_ERROR) && mysql_ping(dbp) == 0 && retry_cnt < RETRY_QUERY_CNT)	//reconnected
			{
				cout<< __func__ << ": mysql_ping OK, retrying" << endl;
				continue;
			}
			else if(retry_cnt == RETRY_QUERY_CNT)
			{
				Tango::Except::throw_exception(QUERY_ERROR,error,__func__);
			}
		}
		else
		{
			stmt_close(cached, __func__, pstmt);
#ifdef _LIB_DEBUG
			//cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
			break;
		}
	} while(retry_cnt < RETRY_QUERY_CNT);

#ifdef _LIB_DEBUG
	//cout << __func__<< ": exiting... " << endl;
#endif
}

void HdbPPMySQL::add_attribute(const string &name, int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/)
{
	ostringstream insert_str;
	ostringstream insert_event_str;
	string facility = HdbppStringUtils::get_only_tango_host(name);
	facility = HdbppStringUtils::add_domain(facility);
	string attr_name = HdbppStringUtils::get_only_attr_name(name);
#ifdef _LIB_DEBUG
	cout<< __func__ << ": name="<<name<<" -> facility="<<facility<<" attr_name="<<attr_name<< endl;
#endif
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
		insert_event_str <<
			"INSERT INTO " << m_dbname << "." << HISTORY_TABLE_NAME << " ("<<HISTORY_COL_ID<<","<<HISTORY_COL_EVENT_ID<<","<<HISTORY_COL_TIME<<")" <<
				" SELECT " << id << "," << HISTORY_EVENT_COL_EVENT_ID << ",NOW(6)" <<
				" FROM " << m_dbname << "." << HISTORY_EVENT_TABLE_NAME << " WHERE " << HISTORY_EVENT_COL_EVENT << " = '" << EVENT_ADD << "'";

		if(mysql_query(dbp, insert_event_str.str().c_str()))
		{
			stringstream tmp;
			tmp << "ERROR in query=" << insert_event_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
			cout << __func__<< ": " << tmp.str() << endl;
			Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
		}
		return;
	}

	//add domain name to fqdn
	string name_ok = string("tango://")+facility+string("/")+attr_name;
	char name_escaped[2 * name_ok.length() + 1];
	mysql_escape_string(name_escaped, name_ok.c_str(), name_ok.length());

	vector<string> exploded_name;
	HdbppStringUtils::string_explode(attr_name,"/",exploded_name);

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
		"INSERT INTO " << m_dbname << "." << CONF_TABLE_NAME << " ("<<CONF_COL_NAME<<","<<CONF_COL_TYPE_ID<<","<<
			CONF_COL_FACILITY<<","<<CONF_COL_DOMAIN<<","<<CONF_COL_FAMILY<<","<<CONF_COL_MEMBER<<","<<CONF_COL_LAST_NAME<<")"<<
			" SELECT '" << name_escaped << "'," << CONF_TYPE_COL_TYPE_ID <<
			",'"<<complete_facility_escaped<<"','"<<domain_escaped<<"','"<<family_escaped<<"','"<<member_escaped<<"','"<<last_name_escaped<<"'"<<
			" FROM " << m_dbname << "." << CONF_TYPE_TABLE_NAME << " WHERE " << CONF_TYPE_COL_TYPE << " = '" << data_type << "'";

	if(mysql_query(dbp, insert_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << insert_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
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
		tmp << "ERROR in query=" << insert_event_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
}

void HdbPPMySQL::update_ttl(const string &name, unsigned int ttl/*hours, 0=infinity*/)
{
	ostringstream update_ttl_str;
	string facility = HdbppStringUtils::get_only_tango_host(name);
	facility = HdbppStringUtils::add_domain(facility);
	string attr_name = HdbppStringUtils::get_only_attr_name(name);

	int id=0;
	int ret = find_attr_id(facility, attr_name, id);
	if(ret < 0)
	{
		stringstream tmp;
		tmp << "ERROR "<<facility<<"/"<<attr_name<<" NOT FOUND";
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(DATA_ERROR,tmp.str(),__func__);
	}

	update_ttl_str <<
			"UPDATE " << m_dbname << "." << CONF_TABLE_NAME << " SET " <<
			CONF_COL_TTL << "=" << ttl <<
			" WHERE " << CONF_COL_ID << "=" << id;

	if(mysql_query(dbp, update_ttl_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << update_ttl_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
	}
}

void HdbPPMySQL::insert_history_event(const string &name, unsigned char event)
{
	ostringstream insert_event_str;
	string facility = HdbppStringUtils::get_only_tango_host(name);
	facility = HdbppStringUtils::add_domain(facility);
	string attr_name = HdbppStringUtils::get_only_attr_name(name);

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
		tmp << "ERROR in query=" << insert_event_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
		cout << __func__<< ": " << tmp.str() << endl;
		Tango::Except::throw_exception(DATA_ERROR,tmp.str(),__func__);
	}
}

bool HdbPPMySQL::supported(HdbppFeatures feature)
{
	auto supported = false;

	switch (feature)
	{
		case HdbppFeatures::TTL: supported = true; break;

		case HdbppFeatures::BATCH_INSERTS: supported = true; break;
	}

	return supported;
}

//=============================================================================
//=============================================================================
template <typename Type> void HdbPPMySQL::store_scalar(const vector<event_values_param<Type> > &event_values, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, const string & table_name, enum_field_types mysql_value_type, bool _is_unsigned)
{
	unsigned int max_size = event_values.size();
	unsigned int insert_size = std::min(max_size, batch_size);
	unsigned int inserted_num = 0;
#ifdef _LIB_DEBUG
	cout << __func__<< ": entering with mysql_thread_id="<<mysql_thread_id(dbp) << " data size=" << max_size << " batch size=" << insert_size << " table_name="<< table_name<< endl;
#endif
	bool detected_insert_time = true;
	bool detected_recv_time = true;
	bool detected_quality = true;
	bool detected_error = true;
	if(autodetectschema)
	{
		auto it = table_column_map.find(table_name+"_"+SC_COL_INS_TIME);
		if(it!=table_column_map.end())
			detected_insert_time = it->second;
		else
			detected_insert_time = false;

		it = table_column_map.find(table_name+"_"+SC_COL_RCV_TIME);
		if(it!=table_column_map.end())
			detected_recv_time = it->second;
		else
			detected_recv_time = false;

		it = table_column_map.find(table_name+"_"+SC_COL_QUALITY);
		if(it!=table_column_map.end())
			detected_quality = it->second;
		else
			detected_quality = false;

		it = table_column_map.find(table_name+"_"+SC_COL_ERROR_DESC_ID);
		if(it!=table_column_map.end())
			detected_error = it->second;
		else
			detected_error = false;
	}
	else if(lightschema)
	{
		detected_insert_time = false;
		detected_recv_time = false;
	}

	auto begin_ev = std::begin(event_values);
	auto end_ev = std::end(event_values);

	auto it_loop = begin_ev;

	do
	{
		ostringstream query_str;
		if(!ignoreduplicates)
			query_str << "INSERT INTO ";
		else
			query_str << "INSERT IGNORE INTO ";
		query_str << m_dbname << "." << table_name <<
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

		query_str << " VALUES";

		for(size_t idx=0; idx < insert_size; ++idx)
		{
			query_str << " (?,FROM_UNIXTIME(?),";
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
			if(idx < insert_size-1)
				query_str << ",";
		}

		int param_count_single = 3;	//param in single value insert
		if(detected_recv_time)
			param_count_single++;
		if(detected_quality)
			param_count_single++;
		if(detected_error)
			param_count_single++;
		if(write_type != Tango::READ)
			param_count_single ++;
		int param_count = param_count_single*insert_size;
		unsigned int retry_cnt=0;
		do
		{
			retry_cnt++;
			MYSQL_STMT	*pstmt{nullptr};
			MYSQL_BIND	plog_bind[param_count];
			my_bool		is_null[3*insert_size];    /* value nullability */ //value_r, value_w, error_desc_id
			my_bool		is_unsigned=_is_unsigned;    /* value unsigned */
			double		double_data[2*insert_size];		// rcv_time, ev_time
			Type		value_data[2*insert_size];		//value_r, value_w
			unsigned long value_data_len[2*insert_size];
			int			int_data[3*insert_size];		//id, quality, error_desc_id

			bool cached = cache_pstmt(query_str.str(),&pstmt,insert_size,__func__);

			memset(plog_bind, 0, sizeof(MYSQL_BIND)*param_count);

			for(size_t chunk_idx=0; chunk_idx < insert_size && it_loop != end_ev; ++chunk_idx)
			{
#ifdef _LIB_DEBUG
				cout << __func__<< ": chunk_idx=" << chunk_idx << " attr_name="<< it_loop->param.attr_name << " it_loop->val_r[0]=" << it_loop->val_r[0]
				<< " it_loop->val_r.size()="<< it_loop->val_r.size() << " it_loop->val_w.size()="<< it_loop->val_w.size() << endl;
#endif
				if(it_loop->val_r.size() >= 1 && !it_loop->param.isNull)
				{
					if(is_nan_or_inf(it_loop->val_r[0]))
					{
						is_null[3*chunk_idx+0]=1;
						value_data[2*chunk_idx+0]=foo_value<Type>();	//useless
					}
					else
					{
						is_null[3*chunk_idx+0]=0;
						value_data[2*chunk_idx+0] = it_loop->val_r[0];
					}
				}
				else
				{
					is_null[3*chunk_idx+0]=1;
					value_data[2*chunk_idx+0]=foo_value<Type>();	//useless
				}

				if(it_loop->val_w.size() >= 1 && !it_loop->param.isNull)
				{
					if(is_nan_or_inf(it_loop->val_w[0]))
					{
						is_null[3*chunk_idx+1]=1;
						value_data[2*chunk_idx+1]=foo_value<Type>();	//useless
					}
					else
					{
						is_null[3*chunk_idx+1]=0;
						value_data[2*chunk_idx+1] = it_loop->val_w[0];
					}
				}
				else
				{
					is_null[3*chunk_idx+1]=1;
					value_data[2*chunk_idx+1]=foo_value<Type>();	//useless
				}
				int ID=cache_ID(it_loop->param.attr_name, __func__);
				int_data[3*chunk_idx+0] = ID;
				if(detected_quality)
					int_data[3*chunk_idx+1] = it_loop->param.quality;
				double_data[2*chunk_idx+0] = it_loop->param.ev_time;
				if(detected_recv_time)
					double_data[2*chunk_idx+1] = it_loop->param.rcv_time;
				int ERR_ID=-1;
				if(detected_error)
				{
					cache_err_id(it_loop->param.error_desc, ERR_ID);
					int_data[3*chunk_idx+2] = ERR_ID;
					if(ERR_ID < 0)
						is_null[3*chunk_idx+2]=1;
					else
						is_null[3*chunk_idx+2]=0;
				}
				it_loop++;
				size_t plog_bind_ind=param_count_single*chunk_idx;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[3*chunk_idx+0];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
				plog_bind[plog_bind_ind].buffer= (void *)&double_data[2*chunk_idx+0];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				if(detected_recv_time)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
					plog_bind[plog_bind_ind].buffer= (void *)&double_data[2*chunk_idx+1];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;
				}
				if(detected_quality)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[3*chunk_idx+1];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;
				}
				if(detected_error)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[3*chunk_idx+2];
					plog_bind[plog_bind_ind].is_null= &is_null[3*chunk_idx+2];
					plog_bind[plog_bind_ind].length= 0;
					plog_bind[plog_bind_ind].is_unsigned= 1;
					plog_bind_ind++;
				}

				bind_value(plog_bind[plog_bind_ind],mysql_value_type,value_data[2*chunk_idx+0],value_data_len[2*chunk_idx+0],is_null[3*chunk_idx+0],is_unsigned);
				plog_bind_ind++;

				bind_value(plog_bind[plog_bind_ind],mysql_value_type,value_data[2*chunk_idx+1],value_data_len[2*chunk_idx+1],is_null[3*chunk_idx+1],is_unsigned);
				plog_bind_ind++;
			}

			if (mysql_stmt_bind_param(pstmt, plog_bind))
			{
				stringstream tmp;
				tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
				cout << __func__<< ": " << tmp.str() << endl;
				stmt_close(cached, __func__, pstmt);
				Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
			}

			if (mysql_stmt_execute(pstmt))
			{
				unsigned int mse = mysql_stmt_errno(pstmt);
				string error = err_stmt_close(cached, __func__, query_str.str(), pstmt);
				if((mse == CR_SERVER_LOST || mse == CR_SERVER_GONE_ERROR) && mysql_ping(dbp) == 0 && retry_cnt < RETRY_QUERY_CNT)	//reconnected
				{
					cout<< __func__ << ": mysql_ping OK, retrying" << endl;
					continue;
				}
				else if(retry_cnt == RETRY_QUERY_CNT)
				{
					Tango::Except::throw_exception(QUERY_ERROR,error,__func__);
				}
			}
			else
			{
#ifdef _LIB_DEBUG
				cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
				stmt_close(cached, __func__, pstmt);
				break;
			}
		} while(retry_cnt < RETRY_QUERY_CNT);
		/*		if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/

		inserted_num += insert_size;
		insert_size = std::min(max_size-inserted_num, batch_size);
	
	} while(insert_size > 0);
}

//=============================================================================
//=============================================================================
template <typename Type> void HdbPPMySQL::store_arrays(const vector<event_values_param<Type> > &event_values, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, const string & table_name, enum_field_types mysql_value_type, bool is_unsigned)
{
	for(const auto &it : event_values)
	{
		store_array(it.param.attr_name,it.val_r,it.val_w,it.param.quality,it.param.error_desc,data_type,write_type,it.param.attr_r_dim,it.param.attr_w_dim,it.param.ev_time,it.param.rcv_time,table_name,mysql_value_type,is_unsigned,it.param.isNull);
	}
}

constexpr unsigned int MAX_INSERT_SIZE = 5000;
template <typename Type> void HdbPPMySQL::store_array(const string &attr, const vector<Type> &value_r, const vector<Type> &value_w, int quality/*ATTR_VALID, ATTR_INVALID, ..*/, const string &error_desc, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, Tango::AttributeDimension attr_r_dim, Tango::AttributeDimension attr_w_dim, double ev_time, double rcv_time, const string &table_name, enum_field_types mysql_value_type, bool _is_unsigned, bool isNull)
{
#ifdef _LIB_DEBUG
	cout << __func__<< ": entering..." << endl;
#endif
	int ID=cache_ID(attr, __func__);
	unsigned int max_size = std::max(value_r.size(),value_w.size());
	unsigned int insert_size = std::min(max_size,MAX_INSERT_SIZE);
	unsigned int inserted_num = 0;

	bool detected_insert_time = true;
	bool detected_recv_time = true;
	bool detected_quality = true;
	bool detected_error = true;
	if(autodetectschema)
	{
		auto it = table_column_map.find(table_name+"_"+ARR_COL_INS_TIME);
		if(it!=table_column_map.end())
			detected_insert_time = it->second;
		else
			detected_insert_time = false;

		it = table_column_map.find(table_name+"_"+ARR_COL_RCV_TIME);
		if(it!=table_column_map.end())
			detected_recv_time = it->second;
		else
			detected_recv_time = false;

		it = table_column_map.find(table_name+"_"+ARR_COL_QUALITY);
		if(it!=table_column_map.end())
			detected_quality = it->second;
		else
			detected_quality = false;

		it = table_column_map.find(table_name+"_"+ARR_COL_ERROR_DESC_ID);
		if(it!=table_column_map.end())
			detected_error = it->second;
		else
			detected_error = false;
	}
	else if(lightschema)
	{
		detected_insert_time = false;
		detected_recv_time = false;
	}
	int ERR_ID=-1;
	if(detected_error)
		cache_err_id(error_desc, ERR_ID);

	
	do
	{
	
		ostringstream query_str;

		if(!ignoreduplicates)
			query_str << "INSERT INTO ";
		else
			query_str << "INSERT IGNORE INTO ";
		query_str << m_dbname << "." << table_name <<
				" (" << ARR_COL_ID << "," << ARR_COL_EV_TIME << ",";
		if(detected_insert_time)
			query_str << ARR_COL_INS_TIME << ",";
		if(detected_recv_time)
			query_str << ARR_COL_RCV_TIME << ",";
		if(detected_quality)
			query_str << ARR_COL_QUALITY << ",";
		if(detected_error)
			query_str << ARR_COL_ERROR_DESC_ID << ",";
		query_str << ARR_COL_IDX << "," << ARR_COL_DIMX_R << "," << ARR_COL_DIMY_R << "," << ARR_COL_VALUE_R;
		if(!(write_type == Tango::READ))	//RW
			query_str << "," << ARR_COL_DIMX_W << "," << ARR_COL_DIMY_W << "," << ARR_COL_VALUE_W;
		query_str << ")";

		query_str << " VALUES ";
		for(size_t idx=0; idx < insert_size; idx++)
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
			if(idx < insert_size-1)
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
		int param_count = param_count_single*insert_size;								//total param
		unsigned int retry_cnt=0;
		do
		{
			retry_cnt++;
			MYSQL_STMT	*pstmt{nullptr};
			MYSQL_BIND	*plog_bind = new MYSQL_BIND[param_count];
			my_bool		is_null[3*insert_size];    /* value nullability */	//value_r, value_w, error_desc_id
			my_bool		is_unsigned=_is_unsigned;    /* value unsigned */
			double		double_data[2*insert_size];	// rcv_time, ev_time
			Type		value_data[2*insert_size];		//value_r, value_w
			unsigned long value_data_len[2*insert_size];
			int		int_data[8*insert_size];		//id, quality, error_desc_id, idx, dimx_r, dimy_r, dimx_w, dimy_w,

			bool cached = cache_pstmt(query_str.str(),&pstmt,1,__func__);

			memset(plog_bind, 0, sizeof(MYSQL_BIND)*param_count);

			for(size_t chunk_idx=0; chunk_idx < insert_size; chunk_idx++)
			{
				size_t idx = inserted_num + chunk_idx;
				if(idx < value_r.size() && !isNull)
				{
					if(is_nan_or_inf(value_r[idx]))
					{
						is_null[3*chunk_idx+0]=1;
						value_data[2*chunk_idx+0]=foo_value<Type>();	//useless
					}
					else
					{
						is_null[3*chunk_idx+0]=0;
						value_data[2*chunk_idx+0] = value_r[idx];
					}
				}
				else
				{
					is_null[3*chunk_idx+0]=1;
					value_data[2*chunk_idx+0]=foo_value<Type>();	//useless
				}

				if(idx < value_w.size() && !isNull)
				{
					if(is_nan_or_inf(value_w[idx]))
					{
						is_null[3*chunk_idx+1]=1;
						value_data[2*chunk_idx+1]=foo_value<Type>();	//useless
					}
					else
					{
						is_null[3*chunk_idx+1]=0;
						value_data[2*chunk_idx+1] = value_w[idx];
					}
				}
				else
				{
					is_null[3*chunk_idx+1]=1;
					value_data[2*chunk_idx+1]=foo_value<Type>();	//useless
				}

				int_data[8*chunk_idx+0] = ID;
				if(detected_quality)
					int_data[8*chunk_idx+1] = quality;
				if(detected_error)
				{
					int_data[8*chunk_idx+2] = ERR_ID;
					if(ERR_ID < 0)
						is_null[3*chunk_idx+2]=1;
					else
						is_null[3*chunk_idx+2]=0;
				}
				int_data[8*chunk_idx+3] = idx;
				int_data[8*chunk_idx+4] = attr_r_dim.dim_x;
				int_data[8*chunk_idx+5] = attr_r_dim.dim_y;
				if(!(write_type == Tango::READ))
				{
					int_data[8*chunk_idx+6] = attr_w_dim.dim_x;
					int_data[8*chunk_idx+7] = attr_w_dim.dim_y;
				}
				double_data[2*chunk_idx+0] = ev_time;
				if(detected_recv_time)
					double_data[2*chunk_idx+1] = rcv_time;

				size_t plog_bind_ind = param_count_single*chunk_idx;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*chunk_idx+0];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
				plog_bind[plog_bind_ind].buffer= (void *)&double_data[2*chunk_idx+0];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				if(detected_recv_time)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
					plog_bind[plog_bind_ind].buffer= (void *)&double_data[2*chunk_idx+1];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;
				}

				if(detected_quality)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*chunk_idx+1];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;
				}

				if(detected_error)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*chunk_idx+2];
					plog_bind[plog_bind_ind].is_null= &is_null[3*chunk_idx+2];
					plog_bind[plog_bind_ind].length= 0;
					plog_bind[plog_bind_ind].is_unsigned= 1;
					plog_bind_ind++;
				}

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*chunk_idx+3];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*chunk_idx+4];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*chunk_idx+5];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				bind_value(plog_bind[plog_bind_ind],mysql_value_type,value_data[2*chunk_idx+0],value_data_len[2*chunk_idx+0],is_null[3*chunk_idx+0],is_unsigned);
				plog_bind_ind++;

				if(!(write_type == Tango::READ))
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*chunk_idx+6];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;

					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*chunk_idx+7];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;

					bind_value(plog_bind[plog_bind_ind],mysql_value_type,value_data[2*chunk_idx+1],value_data_len[2*chunk_idx+1],is_null[3*chunk_idx+1],is_unsigned);
					plog_bind_ind++;
				}
			}

			if(max_size == 0)
			{
				int idx = 0;

				is_null[3*idx+0]=1;
				value_data[2*idx+0]=foo_value<Type>();	//useless
				is_null[3*idx+1]=1;
				value_data[2*idx+1]=foo_value<Type>();	//useless

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

				size_t plog_bind_ind = param_count_single*idx;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*idx+0];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
				plog_bind[plog_bind_ind].buffer= (void *)&double_data[2*idx+0];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				if(detected_recv_time)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
					plog_bind[plog_bind_ind].buffer= (void *)&double_data[2*idx+1];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;
				}

				if(detected_quality)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*idx+1];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;
				}

				if(detected_error)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*idx+2];
					plog_bind[plog_bind_ind].is_null= &is_null[3*idx+2];
					plog_bind[plog_bind_ind].length= 0;
					plog_bind[plog_bind_ind].is_unsigned= 1;
					plog_bind_ind++;
				}

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*idx+3];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*idx+4];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*idx+5];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				bind_value(plog_bind[plog_bind_ind],mysql_value_type,value_data[2*idx+0],value_data_len[2*idx+0],is_null[3*idx+0],is_unsigned);
				plog_bind_ind++;

				if(!(write_type == Tango::READ))
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*idx+6];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;

					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[8*idx+7];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;

					bind_value(plog_bind[plog_bind_ind],mysql_value_type,value_data[2*idx+1],value_data_len[2*idx+1],is_null[3*idx+1],is_unsigned);
					plog_bind_ind++;
				}
			}

			if (mysql_stmt_bind_param(pstmt, plog_bind))
			{
				stringstream tmp;
				tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
				cout << __func__<< ": " << tmp.str() << endl;
				stmt_close(cached, __func__, pstmt);
				Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
			}

			if (mysql_stmt_execute(pstmt))
			{
				unsigned int mse = mysql_stmt_errno(pstmt);
				string error = err_stmt_close(cached, __func__, query_str.str(), pstmt);
				delete [] plog_bind;
				if((mse == CR_SERVER_LOST || mse == CR_SERVER_GONE_ERROR) && mysql_ping(dbp) == 0 && retry_cnt < RETRY_QUERY_CNT)	//reconnected
				{
					cout<< __func__ << ": mysql_ping OK, retrying" << endl;
					continue;
				}
				else if(retry_cnt == RETRY_QUERY_CNT)
				{
					Tango::Except::throw_exception(QUERY_ERROR,error,__func__);
				}
			}
			else
			{
				stmt_close(cached, __func__, pstmt);
#ifdef _LIB_DEBUG
				cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
				delete [] plog_bind;
				break;
			}
		} while(retry_cnt < RETRY_QUERY_CNT);
/*		if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
		
		inserted_num += insert_size;
		insert_size = std::min(max_size-inserted_num,MAX_INSERT_SIZE);
	
	} while(insert_size > 0);
}

//=============================================================================
//=============================================================================
template <typename Type> void HdbPPMySQL::store_array_json(const vector<event_values_param<Type> > &event_values, int data_type/*DEV_DOUBLE, DEV_STRING, ..*/, int write_type/*READ, READ_WRITE, ..*/, const string & table_name, enum_field_types mysql_value_type, bool is_unsigned)
{
	unsigned int max_size = event_values.size();
	unsigned int insert_size = std::min(max_size,batch_size);
	unsigned int inserted_num = 0;
#ifdef _LIB_DEBUG
	cout << __func__<< ": entering... data_size="<<max_size << " batch size="<< insert_size << " table_name=" << table_name << endl;
#endif

	bool detected_insert_time = true;
	bool detected_recv_time = true;
	bool detected_quality = true;
	bool detected_error = true;
	if(autodetectschema)
	{
		auto it = table_column_map.find(table_name+"_"+ARR_COL_INS_TIME);
		if(it!=table_column_map.end())
			detected_insert_time = it->second;
		else
			detected_insert_time = false;

		it = table_column_map.find(table_name+"_"+ARR_COL_RCV_TIME);
		if(it!=table_column_map.end())
			detected_recv_time = it->second;
		else
			detected_recv_time = false;

		it = table_column_map.find(table_name+"_"+ARR_COL_QUALITY);
		if(it!=table_column_map.end())
			detected_quality = it->second;
		else
			detected_quality = false;

		it = table_column_map.find(table_name+"_"+ARR_COL_ERROR_DESC_ID);
		if(it!=table_column_map.end())
			detected_error = it->second;
		else
			detected_error = false;
	}
	else if(lightschema)
	{
		detected_insert_time = false;
		detected_recv_time = false;
	}

	auto begin_ev = std::begin(event_values);
	auto end_ev = std::end(event_values);

	auto it_loop = begin_ev;

	do
	{
		ostringstream query_str;
		if(!ignoreduplicates)
			query_str << "INSERT INTO ";
		else
			query_str << "INSERT IGNORE INTO ";
		query_str << m_dbname << "." << table_name <<
				" (" << ARR_COL_ID << "," << ARR_COL_EV_TIME << ",";
		if(detected_insert_time)
			query_str << ARR_COL_INS_TIME << ",";
		if(detected_recv_time)
			query_str << ARR_COL_RCV_TIME << ",";
		if(detected_quality)
			query_str << ARR_COL_QUALITY << ",";
		if(detected_error)
			query_str << ARR_COL_ERROR_DESC_ID << ",";		
		query_str << ARR_COL_DIMX_R << "," << ARR_COL_DIMY_R << "," << ARR_COL_VALUE_R;
		if(!(write_type == Tango::READ))	//RW
			query_str << "," << ARR_COL_DIMX_W << "," << ARR_COL_DIMY_W << "," << ARR_COL_VALUE_W;
		query_str << ")";

		query_str << " VALUES ";

		for(size_t idx=0; idx < insert_size; ++idx)
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
			query_str << "?,?,?";
			if(!(write_type == Tango::READ))	//RW
				query_str << ",?,?,?";
			query_str << ")";
			if(idx < insert_size-1)
				query_str << ",";
		}

		int param_count_single = 5;	//param in single value insert
		if(detected_recv_time)
			param_count_single++;
		if(detected_quality)
			param_count_single++;
		if(detected_error)
			param_count_single++;
		if(write_type != Tango::READ)
			param_count_single += 3;
		int param_count = param_count_single*insert_size;				//total param
		unsigned int retry_cnt=0;
		do
		{
			retry_cnt++;
			MYSQL_STMT	*pstmt{nullptr};
			MYSQL_BIND	plog_bind[param_count];
			my_bool		is_null[3*insert_size];    /* value nullability */	//value_r, value_w, error_desc_id
			double		double_data[2*insert_size];	// rcv_time, ev_time
			string		value_data[2*insert_size];		//value_r, value_w
			unsigned long value_data_len[2*insert_size];
			int			int_data[7*insert_size];		//id, quality, error_desc_id, dimx_r, dimy_r, dimx_w, dimy_w,

			bool cached = cache_pstmt(query_str.str(),&pstmt,insert_size,__func__);

			memset(plog_bind, 0, sizeof(MYSQL_BIND)*param_count);

			for(size_t chunk_idx=0; chunk_idx < insert_size && it_loop != end_ev; ++chunk_idx)	
			{
				unsigned int max_array_size = std::max(it_loop->val_r.size(),it_loop->val_w.size());
				ostringstream ss_value_r,ss_value_w;
				for(size_t idx=0; idx < max_array_size; idx++)
				{
					if(idx < it_loop->val_r.size() && !it_loop->param.isNull)
					{
						if(idx == 0)
						{
							ss_value_r << "[";
							is_null[3*chunk_idx+0]=0;
						}
						if(is_nan_or_inf(it_loop->val_r[idx]))
						{
							ss_value_r << "null";
						}
						else
						{
							ss_value_r << to_json_value(it_loop->val_r[idx]);
						}
						if(idx < it_loop->val_r.size()-1)
						{
							ss_value_r << ",";
						}
					}
					else
					{
						is_null[3*chunk_idx+0]=1;
					}
					if(idx < it_loop->val_w.size() && !it_loop->param.isNull)
					{
						if(idx == 0)
						{
							ss_value_w << "[";
							is_null[3*chunk_idx+1]=0;
						}
						if(is_nan_or_inf(it_loop->val_w[idx]))
						{
							ss_value_w << "null";
						}
						else
						{
							ss_value_w << to_json_value(it_loop->val_w[idx]);
						}
						if(idx < it_loop->val_w.size()-1)
						{
							ss_value_w << ",";
						}
					}
					else
					{
						is_null[3*chunk_idx+1]=1;
					}
				}
				if(it_loop->val_r.size() > 0 && !it_loop->param.isNull)
					ss_value_r << "]";
				if(it_loop->val_w.size() > 0 && !it_loop->param.isNull)
					ss_value_w << "]";

#ifdef _LIB_DEBUG
				cout << __func__<< ": value_r=" << ss_value_r.str() << endl;
				cout << __func__<< ": value_w=" << ss_value_w.str() << endl;
#endif

				value_data[2*chunk_idx+0] = ss_value_r.str();
				value_data[2*chunk_idx+1] = ss_value_w.str();
				int ID=cache_ID(it_loop->param.attr_name, __func__);
				int_data[7*chunk_idx+0] = ID;
				if(detected_quality)
					int_data[7*chunk_idx+1] = it_loop->param.quality;
				double_data[2*chunk_idx+0] = it_loop->param.ev_time;
				if(detected_recv_time)
					double_data[2*chunk_idx+1] = it_loop->param.rcv_time;
				int ERR_ID=-1;
				if(detected_error)
				{
					cache_err_id(it_loop->param.error_desc, ERR_ID);
					int_data[7*chunk_idx+2] = ERR_ID;
					if(ERR_ID < 0)
						is_null[3*chunk_idx+2]=1;
					else
						is_null[3*chunk_idx+2]=0;
				}
				int_data[7*chunk_idx+3] = it_loop->param.attr_r_dim.dim_x;
				int_data[7*chunk_idx+4] = it_loop->param.attr_r_dim.dim_y;
				if(!(write_type == Tango::READ))
				{
					int_data[7*chunk_idx+5] = it_loop->param.attr_w_dim.dim_x;
					int_data[7*chunk_idx+6] = it_loop->param.attr_w_dim.dim_y;
				}
				it_loop++;
				size_t plog_bind_ind = param_count_single*chunk_idx;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[7*chunk_idx+0];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
				plog_bind[plog_bind_ind].buffer= (void *)&double_data[2*chunk_idx+0];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				if(detected_recv_time)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_DOUBLE;
					plog_bind[plog_bind_ind].buffer= (void *)&double_data[2*chunk_idx+1];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;
				}

				if(detected_quality)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[7*chunk_idx+1];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;
				}

				if(detected_error)
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[7*chunk_idx+2];
					plog_bind[plog_bind_ind].is_null= &is_null[3*chunk_idx+2];
					plog_bind[plog_bind_ind].length= 0;
					plog_bind[plog_bind_ind].is_unsigned= 1;
					plog_bind_ind++;
				}

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[7*chunk_idx+3];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
				plog_bind[plog_bind_ind].buffer= (void *)&int_data[7*chunk_idx+4];
				plog_bind[plog_bind_ind].is_null= 0;
				plog_bind[plog_bind_ind].length= 0;
				plog_bind_ind++;

				plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_JSON;
				plog_bind[plog_bind_ind].buffer= (void *)value_data[2*chunk_idx+0].c_str();
				plog_bind[plog_bind_ind].is_null= &is_null[3*chunk_idx+0];
				value_data_len[2*chunk_idx+0]=value_data[2*chunk_idx+0].length();
				plog_bind[plog_bind_ind].length= &value_data_len[2*chunk_idx+0];
				plog_bind_ind++;

				if(!(write_type == Tango::READ))
				{
					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[7*chunk_idx+5];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;

					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_LONG;
					plog_bind[plog_bind_ind].buffer= (void *)&int_data[7*chunk_idx+6];
					plog_bind[plog_bind_ind].is_null= 0;
					plog_bind[plog_bind_ind].length= 0;
					plog_bind_ind++;

					plog_bind[plog_bind_ind].buffer_type= MYSQL_TYPE_JSON;
					plog_bind[plog_bind_ind].buffer= (void *)value_data[2*chunk_idx+1].c_str();
					plog_bind[plog_bind_ind].is_null= &is_null[3*chunk_idx+1];
					value_data_len[2*chunk_idx+1]=value_data[2*chunk_idx+1].length();
					plog_bind[plog_bind_ind].length= &value_data_len[2*chunk_idx+1];
					plog_bind_ind++;
				}
			}

			if (mysql_stmt_bind_param(pstmt, plog_bind))
			{
				stringstream tmp;
				tmp << "mysql_stmt_bind_param() failed" << ", err=" << mysql_stmt_error(pstmt);
				cout << __func__<< ": " << tmp.str() << endl;
				stmt_close(cached, __func__, pstmt);
				Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),__func__);
			}

			if (mysql_stmt_execute(pstmt))
			{
				unsigned int mse = mysql_stmt_errno(pstmt);
				string error = err_stmt_close(cached, __func__, query_str.str(), pstmt);
				if((mse == CR_SERVER_LOST || mse == CR_SERVER_GONE_ERROR) && mysql_ping(dbp) == 0 && retry_cnt < RETRY_QUERY_CNT)	//reconnected
				{
					cout<< __func__ << ": mysql_ping OK, retrying" << endl;
					continue;
				}
				else if(retry_cnt == RETRY_QUERY_CNT)
				{
					Tango::Except::throw_exception(QUERY_ERROR,error,__func__);
				}
			}
			else
			{
#ifdef _LIB_DEBUG
				cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
				stmt_close(cached, __func__, pstmt);
				break;
			}
		} while(retry_cnt < RETRY_QUERY_CNT);

/*		if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
		inserted_num += insert_size;
		insert_size = std::min(max_size-inserted_num,batch_size);
	} while(insert_size > 0);
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
	else if(type==Tango::DEV_ENUM)
	{
		data_type << TYPE_DEV_ENUM << "_";
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
bool HdbPPMySQL::autodetect_column(string table_name, string column_name)
{
	ostringstream query_str;
	query_str <<
		"SELECT " << INF_SCHEMA_COLUMN_NAME << " FROM " << INFORMATION_SCHEMA << "." << INF_SCHEMA_COLUMNS << " WHERE " << INF_SCHEMA_TABLE_SCHEMA <<
			"='" << m_dbname << "' AND " << INF_SCHEMA_TABLE_NAME << "='" << table_name << "'";
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		stringstream tmp;
		tmp << "ERROR in query=" << query_str.str() << ", err=" << mysql_errno(dbp) << " - " << mysql_error(dbp);
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

int HdbPPMySQL::cache_ID(const string &attr, const string &func_name)
{
	auto it = attr_ID_map.find(attr);
	//if not already present in cache, look for ID in the DB
	if(it == attr_ID_map.end())
	{
		int ID=-1;
		string facility = HdbppStringUtils::get_only_tango_host(attr);
		string attr_name = HdbppStringUtils::get_only_attr_name(attr);
		find_attr_id(facility, attr_name, ID);
		if(ID != -1)
		{
			attr_ID_map.insert(make_pair(attr,ID));
			it = attr_ID_map.find(attr);
		}
		else
		{
			cout << __func__<< ": ID not found!" << endl;
			Tango::Except::throw_exception(DATA_ERROR,"ID not found",func_name.c_str());
		}
	}
	if(it == attr_ID_map.end())
	{
		cout << __func__<< ": ID not found for attr="<<attr << endl;
		Tango::Except::throw_exception(DATA_ERROR,"ID not found",func_name.c_str());
	}
	return it->second;
}

bool HdbPPMySQL::cache_pstmt(const string &query, MYSQL_STMT **pstmt, unsigned int stmt_size/*TODO: handle max-prepared-stmt-count*/, const string &func_name)
{
	//pstmt = nullptr;
	unsigned long mti = mysql_thread_id(dbp);
	if(db_mti != mti)
	{
#ifdef _LIB_DEBUG
		cout << func_name<< ": changed mysql_thread_id from " << db_mti << " to " << mti << endl;
#endif
		db_mti = mti;
		for(auto it_pstmt : pstmt_map)
		{
			if (mysql_stmt_close(it_pstmt.second))
			{
				stringstream tmp;
				tmp << "failed while closing the statement" << ", err=" << mysql_error(dbp);
				cout << __func__<< ": " << tmp.str() << endl;
			}
		}
		pstmt_map.clear();
	}
	auto it_pstmt = pstmt_map.find(query);
	if(it_pstmt == pstmt_map.end())
	{
		*pstmt = mysql_stmt_init(dbp);
		if(!pstmt)
		{
			stringstream tmp;
			tmp << "mysql_stmt_init(), out of memory";
			cout << __func__<< ": " << tmp.str() << endl;
			Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),func_name);
		}
		if(mysql_stmt_prepare(*pstmt, query.c_str(), query.length()))
		{
			stringstream tmp;
			tmp << "mysql_stmt_prepare(), INSERT failed query=" << query << ", err=" << mysql_stmt_error(*pstmt);
			cout << __func__<< ": " << tmp.str() << endl;
			if (mysql_stmt_close(*pstmt))
				cout << __func__<< ": failed while closing the statement" << ", err=" << mysql_error(dbp) << endl;
			Tango::Except::throw_exception(QUERY_ERROR,tmp.str(),func_name);
		}
		if(stmt_size < 20 || stmt_size == batch_size)	//TODO: cache only statements used more frequently
		{
			pstmt_map.insert(make_pair(query, *pstmt));
		}
		else
		{
			return false;
		}
	}
	else
	{
		*pstmt = it_pstmt->second;
	}
	return true;
}

void HdbPPMySQL::stmt_close(bool cached, const string &func, MYSQL_STMT	*pstmt)
{
	if(!cached)
	{
		if(mysql_stmt_close(pstmt))
		{
			stringstream tmp;
			tmp << "failed while closing the statement" << ", err=" << mysql_error(dbp);
			cout << func << ": " << tmp.str() << endl;
		}
	}
}

string HdbPPMySQL::err_stmt_close(bool cached, const string &func, const string &query, MYSQL_STMT *pstmt)
{
	stringstream tmp;
	tmp << "ERROR in query=" << query << ", err=" << mysql_stmt_error(pstmt);
	cout << func << ": " << tmp.str() << endl;
	stmt_close(cached, func, pstmt);
	return tmp.str();
}

//=============================================================================
//=============================================================================
AbstractDB* HdbPPMySQLFactory::create_db(const string &id, const vector<string> &configuration)
{
	return new hdbpp::HdbPPMySQL(id, configuration);
}

} // namespace hdbpp
//=============================================================================
//=============================================================================
hdbpp::DBFactory *getDBFactory()
{
	auto *db_mysql_factory = new hdbpp::HdbPPMySQLFactory();
	return static_cast<hdbpp::DBFactory*>(db_mysql_factory);
}
