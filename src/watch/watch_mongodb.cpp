#include <cstdlib>
#include <iostream>
#include <sstream>
#include <limits>
#include <stdexcept>
#include <set>
#include <map>
#include <vector>

#include <client/dbclient.h>

#include <boost/lexical_cast.hpp>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/timer/timer.hpp>
#include <boost/locale.hpp>

#include <algorithm>
#include <boost/lambda/lambda.hpp>

#include "asn1.h"

using namespace mongo;
using namespace std;
using namespace boost;
using namespace boost::program_options;
using namespace boost::lambda;

#if 1
#define DBNAME "admin"
#else
#define DBNAME "local"
#endif

extern
void connect(DBClientConnection &c, std::string const &dsn, std::string const &dbname, std::string const &user);

namespace boost
{

template<>
std::string
lexical_cast<std::string>( bool const &v )
{
    return v ? "true" : "false";
}

}

string
join( string const &delim, vector<string> const &list )
{
    string rc;

    for( vector<string>::const_iterator ci = list.begin(); ci != list.end(); ++ci )
    {
        if( !rc.empty() )
            rc += delim;
        rc += *ci;
    }

    return rc;
}

#define EXCEED_TYPE_BOUNDS(TYPE, VAL) \
	std::numeric_limits<TYPE>::is_integer && \
	((VAL > std::numeric_limits<TYPE>::max()) || \
	 (VAL < std::numeric_limits<TYPE>::min()))

template<class T>
T
extract_number(BSONElement const &e, string const &oid)
{
    T result;

    switch( e.type() )
    {
	case NumberDouble:
	{
	    double tmp = e.Double();
	    if( EXCEED_TYPE_BOUNDS(T, tmp) )
		throw out_of_range(
		    string("double value for ") + oid +
		    " (" + lexical_cast<string>(tmp) + ") not between " +
		    lexical_cast<string>(std::numeric_limits<T>::min()) +
		    " and " +
		    lexical_cast<string>(std::numeric_limits<T>::max()));
	    return (T)tmp;
	}
	case Bool:
	{
	    int tmp = e.Bool();
	    return (T)tmp;
	}
	case NumberInt:
	{
	    long long tmp = e.Int();
	    if( EXCEED_TYPE_BOUNDS(T, tmp) )
		throw out_of_range(
		    string("int value for ") + oid +
		    " (" + lexical_cast<string>(tmp) + ") not between " +
		    lexical_cast<string>(std::numeric_limits<T>::min()) +
		    " and " +
		    lexical_cast<string>(std::numeric_limits<T>::max()));
	    return (T)tmp;
	}
	case Date:
	{
	    unsigned long long tmp = e.Date();
	    if( EXCEED_TYPE_BOUNDS(T, tmp) )
		throw out_of_range(
		    string("int value for ") + oid +
		    " (" + lexical_cast<string>(tmp) + ") not between " +
		    lexical_cast<string>(std::numeric_limits<T>::min()) +
		    " and " +
		    lexical_cast<string>(std::numeric_limits<T>::max()));
	    return (T)tmp;
	}
	case Timestamp:
	{
	    if( std::numeric_limits<T>::max() <= std::numeric_limits<unsigned int>::max() )
	    {
		unsigned int tmp = e.timestampInc();
		if( EXCEED_TYPE_BOUNDS(T, tmp) )
		    throw out_of_range(
			string("int value for ") + oid +
			" (" + lexical_cast<string>(tmp) + ") not between " +
			lexical_cast<string>(std::numeric_limits<T>::min()) +
			" and " +
			lexical_cast<string>(std::numeric_limits<T>::max()));
		return (T)tmp;
	    }
	    else
	    {
		unsigned long long tmp = e.timestampTime();
		if( EXCEED_TYPE_BOUNDS(T, tmp) )
		    throw out_of_range(
			string("int value for ") + oid +
			" (" + lexical_cast<string>(tmp) + ") not between " +
			lexical_cast<string>(std::numeric_limits<T>::min()) +
			" and " +
			lexical_cast<string>(std::numeric_limits<T>::max()));
		return (T)tmp;
	    }
	}
	case NumberLong:
	{
	    long long tmp = e.Long();
	    if( EXCEED_TYPE_BOUNDS(T, tmp) )
		throw out_of_range(
		    string("long long value for ") + oid +
		    " (" + lexical_cast<string>(tmp) + ") not between " +
		    lexical_cast<string>(std::numeric_limits<T>::min()) +
		    " and " +
		    lexical_cast<string>(std::numeric_limits<T>::max()));
	    return (T)tmp;
	}
	default:
	{
	    StringBuilder ss;
	    ss << "wrong type for field (" << e.fieldName() << ") " << e.type() << " not in "
	       << NumberDouble << ", "
	       << Bool << ", "
	       << NumberInt << ", "
	       << Timestamp << " or "
	       << NumberLong;
	    msgasserted(13111, ss.str() );
	    break;
	}
    }
}

struct OidValueTuple
{
    string oid;
    unsigned type;
    string value;

    OidValueTuple(string const &an_oid, unsigned a_type = ASN_NULL, string const &a_value = string())
	: oid(an_oid)
	, type(a_type)
	, value(a_value)
    {}
};

inline bool
operator < (OidValueTuple const &x, OidValueTuple const &y)
{
    return x.oid < y.oid;
}

template<class T>
OidValueTuple
extract(BSONElement const &e, string const &oid)
{
    return OidValueTuple( oid );
}

template<>
OidValueTuple
extract<string>(BSONElement const &e, string const &oid)
{
    return OidValueTuple( oid, ASN_OCTET_STR, e.String() );
}

template<>
OidValueTuple
extract<int>(BSONElement const &e, string const &oid)
{
    return OidValueTuple( oid, ASN_INTEGER, lexical_cast<string>( extract_number<int>(e, oid) ) );
}

template<>
OidValueTuple
extract<unsigned int>(BSONElement const &e, string const &oid)
{
    return OidValueTuple( oid, SMI_UINTEGER, lexical_cast<string>( extract_number<unsigned int>(e, oid) ) );
}

template<>
OidValueTuple
extract<unsigned long long>(BSONElement const &e, string const &oid)
{
    return OidValueTuple( oid, SMI_COUNTER64, lexical_cast<string>( extract_number<unsigned long long>(e, oid) ) );
}

template<>
OidValueTuple
extract<double>(BSONElement const &e, string const &oid)
{
    return OidValueTuple( oid, ASN_OCTET_STR, lexical_cast<string>( extract_number<double>(e, oid) ) );
}

struct Extractor
{
public:
    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals) = 0;
};

ostream &
operator << (ostream &os, OidValueTuple const val)
{
    os << "(" << val.oid << ", " << val.type << ", " << val.value << ")";
    return os;
}

template<class T>
struct ItemExtractor
    : public Extractor
{
public:
    ItemExtractor(string const &oid)
	: Extractor()
	, m_oid(oid)
    {}

    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals)
    {
	out_vals.insert( extract<T>( e, m_oid ) );
    }

protected:
    string const m_oid;

private:
    ItemExtractor();
};

struct StructExtractor
    : public Extractor
{
public:
    StructExtractor(map<string, Extractor *> *item_rules)
	: Extractor()
	, m_item_rules(item_rules)
    {}

    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals)
    {
        BSONObjIterator i(e.Obj());
        while( i.more() )
	{
            BSONElement elem = i.next();
            string fname = elem.fieldName();

	    map<string, Extractor *>::iterator iter = m_item_rules->find( fname );
	    if( iter != m_item_rules->end() )
	    {
		Extractor &ex = *iter->second;
		ex(elem, out_vals);
	    }
        }
    }

    virtual void operator()(BSONObj const &o, set<OidValueTuple> &out_vals)
    {
        BSONObjIterator i(o.begin());
        while( i.more() )
	{
            BSONElement elem = i.next();
            string fname = elem.fieldName();

	    map<string, Extractor *>::iterator iter = m_item_rules->find( fname );
	    if( iter != m_item_rules->end() )
	    {
		Extractor &ex = *iter->second;
		ex(elem, out_vals);
	    }
        }
    }

    virtual ~StructExtractor()
    {
	for( map<string, Extractor *>::iterator i = m_item_rules->begin();
	     i != m_item_rules->end();
	     ++i )
	{
	    delete i->second;
	    i->second = 0;
	}

	delete m_item_rules;
    }

protected:
    map<string, Extractor *> *m_item_rules;

private:
    StructExtractor();
    StructExtractor(StructExtractor const &);
    StructExtractor & operator = (StructExtractor const &);
};

struct Anyfix
{
    virtual string operator()() const = 0;
};

struct StaticAnyfix
    : public Anyfix
{
    StaticAnyfix(string const &anyfix)
	: Anyfix()
	, m_anyfix(anyfix)
    {}

    virtual string operator()() const { return m_anyfix; }

protected:
     string m_anyfix;
};

#if 0
template<class Key, class Compare = less<Key>, class Allocator = allocator<Key> >
std::set<Key, Compare, Allocator>::iterator
insert_or_update( std::set<Key, Compare, Allocator> &vals, Key const &v )
{
    std::set<Key, Compare, Allocator>::iterator i = vals.lower_bound(v);
    if( ( i == vals.end() ) || ( vals.key_comp()(v, *i ) ) )
    {
	i = vals.insert( i, v );
    }
    else
    {
	Key &ev = const_cast<Key &>(*i);
	ev = v;
    }

    return i;
}
#endif

template<class Embed, class Pre, class Post>
struct BothfixStructExtractor
    : public Embed
{
public:
    BothfixStructExtractor(Pre prefix, Post postfix)
	: Embed()
	, m_prefix(prefix)
	, m_postfix(postfix)
    {}

    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals)
    {
	set<OidValueTuple> ofs_vals;
	Embed::operator()(e, ofs_vals);
	apply_fixes(ofs_vals, out_vals);
    }

    void apply_fixes(set<OidValueTuple> &collected_vals, set<OidValueTuple> &out_vals)
    {
	for( set<OidValueTuple>::iterator iter = collected_vals.begin();
	     iter != collected_vals.end();
	     ++iter )
	{
	    OidValueTuple ov = *iter;
	    ov.oid = m_prefix() + ov.oid + m_postfix();
	    insert_or_update( out_vals, ov );
	}
    }

protected:
    Pre m_prefix;
    Post m_postfix;

    std::set<OidValueTuple>::iterator
    insert_or_update( std::set<OidValueTuple> &vals, OidValueTuple const &v )
    {
	std::set<OidValueTuple>::iterator i = vals.lower_bound(v);
	if( ( i == vals.end() ) || ( vals.key_comp()(v, *i ) ) )
	{
	    i = vals.insert( i, v );
	}
	else
	{
	    OidValueTuple &ev = const_cast<OidValueTuple &>(*i);
	    ev = v;
	}

	return i;
    }

private:
    BothfixStructExtractor();
    BothfixStructExtractor(StructExtractor const &);
    BothfixStructExtractor & operator = (BothfixStructExtractor const &);
};

struct RowPostfix
    : public Anyfix
{
    friend void swap(RowPostfix &a, RowPostfix &b);

public:
    RowPostfix(unsigned row = 0)
	: Anyfix()
	, m_row(row)
    {}

    virtual string operator()() const { return "." + lexical_cast<string>(m_row); }

    operator unsigned() const { return m_row; }

    unsigned getRow() const { return m_row; }
    unsigned nextRow() { return ++m_row; }
    unsigned setNextRow( unsigned next_row ) { return m_row = next_row; }

protected:
    unsigned m_row;
};

void
swap(RowPostfix &a, RowPostfix &b)
{
    swap(a.m_row, b.m_row);
}

template<class T>
struct ServReplHostsExtractor
    : public ItemExtractor<T>
{
public:
    ServReplHostsExtractor(string const &type)
	: ItemExtractor<T>(".2")
	, m_type(type)
    {}

    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals)
    {
	ItemExtractor<T>::operator()( e, out_vals );

	out_vals.insert( OidValueTuple( ".5", ASN_OCTET_STR, m_type ) );
    }

protected:
    string m_type;
};

struct ServReplWorkersExtractor
    : public ServReplHostsExtractor<string>
{
    ServReplWorkersExtractor()
	: ServReplHostsExtractor<string>("PRIORSEC")
    {}
};

struct ServReplArbiterExtractor
    : public ServReplHostsExtractor<string>
{
    ServReplArbiterExtractor()
	: ServReplHostsExtractor<string>("ARBITER")
    {}
};

template <class E>
struct TableRowExtractor
    : public BothfixStructExtractor<E, const StaticAnyfix, RowPostfix &>
{
public:
    TableRowExtractor(string const &tblOid, RowPostfix &rowPostfix, vector<string> const &key_chk = vector<string>() )
	: BothfixStructExtractor<E, const StaticAnyfix, RowPostfix &>(StaticAnyfix(tblOid + ".1"), rowPostfix)
	, m_key_chk(key_chk)
    {}

    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals)
    {
	unsigned merge_row;
	set<OidValueTuple> embed_vals;

	E::operator()(e, embed_vals);
	if( ( merge_row = find_key(embed_vals, out_vals) ) > 0 )
	{
	    RowPostfix save(merge_row);

	    swap(save, this->m_postfix);
	    this->apply_fixes(embed_vals, out_vals);
	    swap(save, this->m_postfix);
	}
	else
	{
	    if( merge_row < 0 )
		this->m_postfix.setNextRow( 0 - ( merge_row - 1 ) );
	    else
		this->m_postfix.nextRow();
	    this->apply_fixes(embed_vals, out_vals);
	}
    }

    unsigned find_key(set<OidValueTuple> const &embed_vals, set<OidValueTuple> &out_vals) const
    {
	vector<bool> found;
	for( vector<string>::const_iterator ci = m_key_chk.begin();
	     ci != m_key_chk.end();
	     ++ci )
	{
	    OidValueTuple search_key( *ci, ASN_OCTET_STR );
	    set<OidValueTuple>::iterator cmp_iter = embed_vals.lower_bound(search_key);
	    if( cmp_iter == embed_vals.end() )
		continue;

	    search_key.value = cmp_iter->value;
	    search_key.oid = this->m_prefix() + search_key.oid + ".";

	    for( cmp_iter = out_vals.lower_bound(search_key);
		 ( cmp_iter != out_vals.end() ) && ( 0 == cmp_iter->oid.compare( 0, search_key.oid.length(), search_key.oid ) );
		 ++cmp_iter )
	    {
		if( cmp_iter->value == search_key.value )
		{
		    string row_str = cmp_iter->oid.substr( cmp_iter->oid.find_last_of("." ) + 1 );
		    unsigned row = lexical_cast<unsigned>(row_str);
		    if( found.size() < (row+1) )
			found.resize(row+1);

		    if( ci == m_key_chk.begin() )
			found[row] = true;
		    else
			found[row] = found[row] & true;
		}
	    }
	}

	// scan found for first full matching row
	unsigned row = 0;
	for( vector<bool>::iterator vbi = found.begin();
	     vbi != found.end();
	     ++vbi, ++row )
	{
	    if( *vbi )
		return row;
	}

	return 0;
    }

protected:
    vector<string> m_key_chk;

private:
    TableRowExtractor();
};

template<class E>
struct ListRowExtractor
    : public TableRowExtractor<E>
{
public:
    ListRowExtractor( string const &tblOid, RowPostfix &rowPostfix = RowPostfix(), vector<string> const &key_chk = vector<string>() )
	: TableRowExtractor<E>( tblOid, rowPostfix, key_chk )
    {}

    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals)
    {
        BSONObjIterator i(e.Obj());
        while( i.more() )
	{
            BSONElement elem = i.next();
	    TableRowExtractor<E>::operator()(elem, out_vals);
        }
    }
};

Extractor *
global_lock_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "totalTime", new ItemExtractor<unsigned long long>( ".10.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "lockTime", new ItemExtractor<unsigned long long>( ".10.2" ) ) );

    map<string, Extractor *> *current_queue_map = new map<string, Extractor *>;
    current_queue_map->insert( make_pair<string, Extractor *>( "total", new ItemExtractor<unsigned long long>( ".10.3.1" ) ) );
    current_queue_map->insert( make_pair<string, Extractor *>( "readers", new ItemExtractor<unsigned long long>( ".10.3.2" ) ) );
    current_queue_map->insert( make_pair<string, Extractor *>( "writers", new ItemExtractor<unsigned long long>( ".10.3.3" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "lockTime", new StructExtractor(current_queue_map) ) );

    map<string, Extractor *> *active_clients_map = new map<string, Extractor *>;
    active_clients_map->insert( make_pair<string, Extractor *>( "total", new ItemExtractor<unsigned long long>( ".10.4.1" ) ) );
    active_clients_map->insert( make_pair<string, Extractor *>( "readers", new ItemExtractor<unsigned long long>( ".10.4.2" ) ) );
    active_clients_map->insert( make_pair<string, Extractor *>( "writers", new ItemExtractor<unsigned long long>( ".10.4.3" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "lockTime", new StructExtractor(active_clients_map) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
mem_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "bits", new ItemExtractor<unsigned int>( ".11.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "resident", new ItemExtractor<unsigned long long>( ".11.2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "virtual", new ItemExtractor<unsigned long long>( ".11.3" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "supported", new ItemExtractor<unsigned long long>( ".11.4" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "mapped", new ItemExtractor<unsigned long long>( ".11.5" ) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
connections_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "current", new ItemExtractor<unsigned int>( ".12.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "available", new ItemExtractor<unsigned long long>( ".12.2" ) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
bg_flush_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "flushes", new ItemExtractor<unsigned long long>( ".13.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "total_ms", new ItemExtractor<unsigned long long>( ".13.2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "average_ms", new ItemExtractor<unsigned long long>( ".13.3" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "last_ms", new ItemExtractor<unsigned long long>( ".13.4" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "last_finished", new ItemExtractor<unsigned long long>( ".13.5" ) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
cursors_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "totalOpen", new ItemExtractor<unsigned long long>( ".14.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "clientCursors_size", new ItemExtractor<unsigned long long>( ".14.2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "timedOut", new ItemExtractor<unsigned long long>( ".14.3" ) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
network_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "bytesIn", new ItemExtractor<unsigned long long>( ".15.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "bytesOut", new ItemExtractor<unsigned long long>( ".15.2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "numRequests", new ItemExtractor<unsigned long long>( ".15.3" ) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
opcounters_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "insert", new ItemExtractor<unsigned long long>( ".16.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "query", new ItemExtractor<unsigned long long>( ".16.2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "update", new ItemExtractor<unsigned long long>( ".16.3" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "delete", new ItemExtractor<unsigned long long>( ".16.4" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "getmore", new ItemExtractor<unsigned long long>( ".16.5" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "command", new ItemExtractor<unsigned long long>( ".16.6" ) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
asserts_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "regular", new ItemExtractor<unsigned long long>( ".17.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "warning", new ItemExtractor<unsigned long long>( ".17.2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "msg", new ItemExtractor<unsigned long long>( ".17.3" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "user", new ItemExtractor<unsigned long long>( ".17.4" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "rollovers", new ItemExtractor<unsigned long long>( ".17.5" ) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
record_stats_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "accessesNotInMemory", new ItemExtractor<unsigned long long>( ".18.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "pageFaultExceptionsThrown", new ItemExtractor<unsigned long long>( ".18.2" ) ) );

    return new StructExtractor(extractor_map);
}

struct LocksExtractor
    : public StructExtractor
{
public:
    LocksExtractor(int row)
	: StructExtractor(get_extractor_map(row))
    {}

protected:
    static map<string, Extractor *> *
    get_extractor_map(int row)
    {
	map<string, Extractor *> *overall_map = new map<string, Extractor *>;

	if( 0 == row )
	{
	    map<string, Extractor *> *details_map = new map<string, Extractor *>;
	    details_map->insert( make_pair<string, Extractor *>( "R", new ItemExtractor<unsigned long long>( ".19.1.1" ) ) );
	    details_map->insert( make_pair<string, Extractor *>( "W", new ItemExtractor<unsigned long long>( ".19.1.2" ) ) );
	    overall_map->insert( make_pair<string, Extractor *>( "timeLockedMicros", new StructExtractor(details_map) ) );

	    details_map = new map<string, Extractor *>;
	    details_map->insert( make_pair<string, Extractor *>( "R", new ItemExtractor<unsigned long long>( ".19.2.1" ) ) );
	    details_map->insert( make_pair<string, Extractor *>( "W", new ItemExtractor<unsigned long long>( ".19.2.2" ) ) );
	    overall_map->insert( make_pair<string, Extractor *>( "timeAcquiringMicros", new StructExtractor(details_map) ) );
	}
	else
	{
	    map<string, Extractor *> *details_map = new map<string, Extractor *>;
	    details_map->insert( make_pair<string, Extractor *>( "r", new ItemExtractor<unsigned long long>( ".21.1.14." + lexical_cast<string>(row) ) ) );
	    details_map->insert( make_pair<string, Extractor *>( "w", new ItemExtractor<unsigned long long>( ".21.1.15." + lexical_cast<string>(row) ) ) );
	    overall_map->insert( make_pair<string, Extractor *>( "timeLockedMicros", new StructExtractor(details_map) ) );

	    details_map = new map<string, Extractor *>;
	    details_map->insert( make_pair<string, Extractor *>( "r", new ItemExtractor<unsigned long long>( ".21.1.16." + lexical_cast<string>(row) ) ) );
	    details_map->insert( make_pair<string, Extractor *>( "w", new ItemExtractor<unsigned long long>( ".21.1.17." + lexical_cast<string>(row) ) ) );
	    overall_map->insert( make_pair<string, Extractor *>( "timeAcquiringMicros", new StructExtractor(details_map) ) );
	}

	return overall_map;
    }
};


Extractor *
locks_extractors(vector<string> const &dbnames)
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( ".", new LocksExtractor(0) ) );
    unsigned row = 1;
    for( vector<string>::const_iterator ci = dbnames.begin(); ci != dbnames.end(); ++ci, ++row )
	extractor_map->insert( make_pair<string, Extractor *>( *ci, new LocksExtractor(row) ) );

    return new StructExtractor(extractor_map);
}

StructExtractor *
serv_info_repl_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "setName", new ItemExtractor<string>( ".20.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "ismaster", new ItemExtractor<int>( ".20.2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "secondary", new ItemExtractor<int>( ".20.3" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "me", new ItemExtractor<string>( ".20.4" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "primary", new ItemExtractor<string>( ".20.5" ) ) );

    static RowPostfix repl_rows;
    vector<string> repl_tbl;
    repl_tbl.push_back(".2");
    extractor_map->insert( make_pair<string, Extractor *>(
	"hosts", new ListRowExtractor<ServReplWorkersExtractor>(".20.7", repl_rows, repl_tbl) ) );
    extractor_map->insert( make_pair<string, Extractor *>(
	"arbiters", new ListRowExtractor<ServReplArbiterExtractor>(".20.7", repl_rows, repl_tbl) ) );

    return new StructExtractor(extractor_map);
}

Extractor *
repl_network_queue_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "waitTimeMs", new ItemExtractor<unsigned long long>( ".20.6.1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "numElems", new ItemExtractor<unsigned long long>( ".20.6.2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "numBytes", new ItemExtractor<unsigned long long>( ".20.6.3" ) ) );

    return new StructExtractor(extractor_map);
}

StructExtractor *
server_status_extractors(vector<string> const &dbnames)
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    extractor_map->insert( make_pair<string, Extractor *>( "host", new ItemExtractor<string>( ".1" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "version", new ItemExtractor<string>( ".2" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "process", new ItemExtractor<string>( ".3" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "pid", new ItemExtractor<int>( ".4" ) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "uptimeMillis", new ItemExtractor<unsigned long long>( ".5" ) ) );

    extractor_map->insert( make_pair<string, Extractor *>( "globalLock", global_lock_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "mem", mem_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "connections", connections_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "backgroundFlushing", bg_flush_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "cursors", cursors_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "network", network_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "opcounters", opcounters_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "asserts", asserts_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "recordStats", record_stats_extractors() ) );

/*  XXX
		if( serv_status.hasField("locks") && serv_status["locks"].Obj().hasField(dbname.value.c_str()) );
		{
		    BSONObj dbl = serv_status["locks"].Obj().getField(dbname.value).Obj();

		    if( dbl.hasField("timeLockedMicros") )
		    {
			BSONObj o3 = dbl["timeLockedMicros"].Obj();

			if( o3.hasField("r") )
			    out_vals.insert( extract_uint64( o3["r"], string(".21.1.14.") + row_str ) );
			if( o3.hasField("w") )
			    out_vals.insert( extract_uint64( o3["w"], string(".21.1.15.") + row_str ) );
		    }

		    if( dbl.hasField("timeAcquiringMicros") )
		    {
			BSONObj o3 = dbl["timeAcquiringMicros"].Obj();

			if( o3.hasField("r") )
			    out_vals.insert( extract_uint64( o3["r"], string(".21.1.16.") + row_str ) );
			if( o3.hasField("w") )
			    out_vals.insert( extract_uint64( o3["w"], string(".21.1.17.") + row_str ) );
		    }
		}
*/
    extractor_map->insert( make_pair<string, Extractor *>( "locks", locks_extractors(dbnames) ) );
    extractor_map->insert( make_pair<string, Extractor *>( "repl", serv_info_repl_extractors() ) );
    extractor_map->insert( make_pair<string, Extractor *>( "replNetworkQueue", repl_network_queue_extractors() ) );
    // extractor_map->insert( make_pair<string, Extractor *>( "indexCounters", index_cOunters_extractors() ) );

    return new StructExtractor(extractor_map);
}

template<class T1, class T2>
struct DualItemExtractor
    : public Extractor
{
public:
    DualItemExtractor(string const &oid1, string const &oid2)
	: Extractor()
	, m_oid1(oid1)
	, m_oid2(oid2)
    {}

    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals)
    {
	out_vals.insert( extract<T1>( e, m_oid1 ) );
	out_vals.insert( extract<T2>( e, m_oid2 ) );
    }

protected:
    string const m_oid1;
    string const m_oid2;

private:
    DualItemExtractor();
};

struct ReplSetMemberRowExtractor
    : public StructExtractor
{
public:
    ReplSetMemberRowExtractor()
	: StructExtractor(get_extractor_map())
    {}

protected:
    static map<string, Extractor *> *
    get_extractor_map()
    {
	map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

	extractor_map->insert( make_pair<string, Extractor *>( "_id", new ItemExtractor<unsigned>( ".1" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "name", new ItemExtractor<string>( ".2" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "health", new ItemExtractor<double>( ".3" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "state", new ItemExtractor<unsigned>( ".4" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "stateStr", new ItemExtractor<string>( ".5" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "uptime", new ItemExtractor<unsigned long long>( ".6" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>(
	    "optime", new DualItemExtractor<unsigned long long, unsigned>( ".7", ".8" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "pingMs", new ItemExtractor<unsigned long long>( ".9" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "lastHeartbeat", new ItemExtractor<unsigned long long>( ".10" ) ) );

	return extractor_map;
    }
};

StructExtractor *
repl_set_status_extractors()
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    static RowPostfix repl_rows;
    vector<string> repl_tbl;
    repl_tbl.push_back(".2");
    extractor_map->insert( make_pair<string, Extractor *>(
	"members", new ListRowExtractor<ReplSetMemberRowExtractor>(".20.7", repl_rows, repl_tbl) ) );

    return new StructExtractor(extractor_map);
}

struct DatabasesMemberRowExtractor
    : public StructExtractor
{
public:
    DatabasesMemberRowExtractor()
	: StructExtractor(get_extractor_map())
	, m_conn(NULL)
	, m_cmd(BSONObjBuilder().append("dbstats", 1).obj())
	, m_dbinfo()
	, m_dbextractor(get_dbinfo_map())
    {}

    DatabasesMemberRowExtractor(DBClientConnection *conn)
	: StructExtractor(get_extractor_map())
	, m_conn(conn)
	, m_cmd(BSONObjBuilder().append("dbstats", 1).obj())
	, m_dbinfo()
	, m_dbextractor(get_dbinfo_map())
    {}

    virtual void operator()(BSONElement const &e, set<OidValueTuple> &out_vals)
    {
	StructExtractor::operator() (e, out_vals);

	OidValueTuple search_key( ".1" ); // ASN.1 type doesn't matter, only OID
	set<OidValueTuple>::iterator cmp_iter = out_vals.lower_bound(search_key);
	if( m_conn && (cmp_iter != out_vals.end()) )
	{
	    m_conn->runCommand(cmp_iter->value, m_cmd, m_dbinfo);
	    m_dbextractor(m_dbinfo, out_vals);
	}
    }

    virtual void operator()(BSONObj const &o, set<OidValueTuple> &out_vals)
    {
	StructExtractor::operator() (o, out_vals);

	OidValueTuple search_key( ".1" ); // ASN.1 type doesn't matter, only OID
	set<OidValueTuple>::iterator cmp_iter = out_vals.lower_bound(search_key);
	if( m_conn && (cmp_iter != out_vals.end()) )
	{
	    m_conn->runCommand(cmp_iter->value, m_cmd, m_dbinfo);
	    m_dbextractor(m_dbinfo, out_vals);
	}
    }

    void set_conn(DBClientConnection *conn) { m_conn = conn; }

protected:
    DBClientConnection *m_conn;
    BSONObj m_cmd, m_dbinfo;
    StructExtractor m_dbextractor;

    static map<string, Extractor *> *
    get_extractor_map()
    {
	map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

	extractor_map->insert( make_pair<string, Extractor *>( "name", new ItemExtractor<string>( ".1" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "sizeOnDisk", new ItemExtractor<unsigned long long>( ".2" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "empty", new ItemExtractor<int>( ".3" ) ) );

	return extractor_map;
    }

    static map<string, Extractor *> *
    get_dbinfo_map()
    {
	map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

	extractor_map->insert( make_pair<string, Extractor *>( "collections", new ItemExtractor<unsigned long long>( ".4" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "objects", new ItemExtractor<unsigned long long>( ".5" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "avgObjSize", new ItemExtractor<double>( ".6" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "dataSize", new ItemExtractor<unsigned long long>( ".7" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "storageSize", new ItemExtractor<unsigned long long>( ".8" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "numExtents", new ItemExtractor<unsigned>( ".9" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "indexes", new ItemExtractor<unsigned>( ".10" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "sizeOnDisk", new ItemExtractor<unsigned>( ".11" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "fileSize", new ItemExtractor<unsigned long long>( ".12" ) ) );
	extractor_map->insert( make_pair<string, Extractor *>( "nsSizeMB", new ItemExtractor<unsigned>( ".13" ) ) );

	return extractor_map;
    }
};

StructExtractor *
databases_extractors(DBClientConnection &conn)
{
    map<string, Extractor *> *extractor_map = new map<string, Extractor *>;

    static RowPostfix db_rows;
    vector<string> db_tbl;
    db_tbl.push_back(".1");
    ListRowExtractor<DatabasesMemberRowExtractor> *lre = new ListRowExtractor<DatabasesMemberRowExtractor>(".21", db_rows, db_tbl);
    lre->set_conn(&conn);
    extractor_map->insert( make_pair<string, Extractor *>( "databases", lre ) );

    return new StructExtractor(extractor_map);
}

void
collect(DBClientConnection &c, set<OidValueTuple> &out_vals)
{
    BSONObj serv_status, dbases, repl_info, cmd;
    StructExtractor *bson_extractor = 0;

    cmd = BSONObjBuilder().append("listDatabases", 1).obj();
    c.runCommand(DBNAME, cmd, dbases);

    bson_extractor = databases_extractors(c);
    (*bson_extractor)(dbases, out_vals);

    // XXX extract row + dbname for serv_status.locks[]
    vector<string> database_names;
    OidValueTuple search_key(".21.1.1.");
    for( set<OidValueTuple>::iterator cmp_iter = out_vals.lower_bound(search_key);
         ( cmp_iter != out_vals.end() ) && ( 0 == cmp_iter->oid.compare( 0, search_key.oid.length(), search_key.oid ) );
	 ++cmp_iter )
    {
	database_names.push_back(cmp_iter->value);
    }

    cmd = BSONObjBuilder().append( "serverStatus", 1 ).obj();
    c.runCommand(DBNAME, cmd, serv_status);

    bson_extractor = server_status_extractors(database_names);
    (*bson_extractor)(serv_status, out_vals);

    cmd = BSONObjBuilder().append("replSetGetStatus", 1).obj();
    c.runCommand(DBNAME, cmd, repl_info);

    bson_extractor = repl_set_status_extractors();
    (*bson_extractor)(repl_info, out_vals);
}

void
dump(set<OidValueTuple> const &out_vals)
{
    vector<string> result;
    result.reserve(out_vals.size() + 2);
    for( set<OidValueTuple>::iterator iter = out_vals.begin();
         iter != out_vals.end();
	 ++iter )
    {
	std::string s = "  [ \"";
	
	s += iter->oid;
	s += "\", ";
	s += lexical_cast<string>(iter->type);
	s += ", ";
	if( ASN_OCTET_STR == iter->type )
	    s += "\"";
	if( ASN_NULL == iter->type )
	    s += "null";
	else if( ASN_OCTET_STR == iter->type )
	    s += boost::locale::conv::utf_to_utf<char>(iter->value);
	else
	    s += iter->value;
	if( ASN_OCTET_STR == iter->type )
	    s += "\"";
	s += " ]";

	result.push_back(s);
    }

    cout << "[" << endl
	 << join(",\n", result) << endl
	 << "]" << endl;
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

	set<OidValueTuple> out_vals;
	do {
	    DBClientConnection c;

	    boost::timer::cpu_timer db_dur;
	    db_dur.start();
	    connect(c, vm["dsn"].as<string>(), DBNAME, "admin");
	    collect(c, out_vals);
	    db_dur.stop();

	    OidValueTuple val( ".99.1", SMI_COUNTER64 );
	    val.value = lexical_cast<string>( db_dur.elapsed().user );
	    out_vals.insert( val );

	    val.oid = ".99.2";
	    val.value = lexical_cast<string>( db_dur.elapsed().system );
	    out_vals.insert( val );

	    val.oid = ".99.3";
	    val.value = lexical_cast<string>( db_dur.elapsed().wall );
	    out_vals.insert( val );
	} while(0);

	dump(out_vals);
    }
    catch( DBException &e )
    {
	cout << "caught " << e.what() << endl;
    }

    return 0;
}
