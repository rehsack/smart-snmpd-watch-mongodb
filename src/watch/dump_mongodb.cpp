#include <iostream>
#include <client/dbclient.h>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

using namespace mongo;
using namespace std;
using namespace boost;
using namespace boost::program_options;

#if 1
#define DBNAME "admin"
#else
#define DBNAME "local"
#endif

extern
void connect(DBClientConnection &c, std::string const &dsn, std::string const &dbname, std::string const &user);

void
info(DBClientConnection &c)
{
    BSONObj info, cmd;

    cmd = BSONObjBuilder().append( "serverStatus", 1 ).obj();
    c.runCommand(DBNAME, cmd, info);
    cout << info << endl;

    cmd = BSONObjBuilder().append("replSetGetStatus", 1).obj();
    c.runCommand(DBNAME, cmd, info);
    cout << info << endl;

    cmd = BSONObjBuilder().append("listDatabases", 1).obj();
    c.runCommand(DBNAME, cmd, info);
    cout << info << endl;

    // foreach db in dbases { runCommand(db, "dbstats") }
    cmd = BSONObjBuilder().append("dbstats", 1).obj();
    cout << info["databases"].size() << endl;
    vector<BSONElement> dbases = info["databases"].Array();
    cout << dbases.size() << " -- " << dbases.capacity() << endl;
    for( vector<BSONElement>::iterator iter = dbases.begin();
         iter != dbases.end();
	 ++iter )
    {
	if( iter->ok() )
	{
	    BSONObj dbinfo;
	    BSONElement &db = *iter;
	    string dbname;

	    if( !db["name"].ok() )
		continue;

	    db["name"].Val(dbname);
	    cout << dbname << endl;
	    c.runCommand(dbname, cmd, dbinfo);
	    cout << dbinfo << endl;
	}
    }
}

int
main(int argc, char *argv[])
{
    try
    {
	options_description desc("Allowed options");
	desc.add_options()
	    ("help", "produce help message")
	    ("dsn", value<string>(), "set mongodb dsn")
	    ;
	variables_map vm;
	store( parse_command_line( argc, argv, desc ), vm );
	notify(vm);

	if( vm.count("help") )
	{
	    cout << desc << endl;
	    return 1;
	}

	if( vm.count("dsn") == 0 )
	{
	    cerr << desc << endl;
	    return 255;
	}

	DBClientConnection c;

	connect(c, vm["dsn"].as<string>(), DBNAME, "admin");
	info(c);
    }
    catch( DBException &e )
    {
	cout << "caught " << e.what() << endl;
    }

    return 0;
}
