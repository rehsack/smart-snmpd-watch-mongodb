#include <client/dbclient.h>

#include "mongo_pw.cpp"

using namespace mongo;

void
connect(DBClientConnection &c,
        std::string const &dsn,
	std::string const &dbname,
	std::string const &user)
{
    std::string pw = summarize( get_summarizers() );

    std::string errmsg;
    c.connect(dsn);
    c.auth(dbname, user, pw, errmsg);
}


