/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  BTreeIndex tree; // BTree index for the case that index exists
  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }
  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
  if (tree.open(table + ".idx", 'w') == 0)
  {
	  //create key for locate the starting point of the constraint
	  int minKey = 0;
	  //creaste cursor for reading forward
	  IndexCursor cursor;
	  //find the minimum key to read
	  for (int i = 0; i < cond.size(); i++)
	  {
		  //if the comparison is on key, fin the min requirement; otherwise, skip
		  if (cond[i].attr == 1)
		  {
			  //in the case of Equal, set the minKey to equal value and break
			  if (cond[i].comp == SelCond::EQ)
			  {
				  minKey = atoi(cond[i].value);
				  break;
			  }
			  //in the case of >=, set the minKey to the value of the condition
			  else if (cond[i].comp == SelCond::GE) minKey = minKey < atoi(cond[i].value) ? atoi(cond[i].value) : minKey;
			  //in the case of >, set the minKey to the value of the condition with plus one
			  else if (cond[i].comp == SelCond::GT) minKey = minKey < atoi(cond[i].value) + 1 ? atoi(cond[i].value) + 1 : minKey;
		  }
	  }
	  //locate the position of minimum key, and store it in the cursor
	  tree.locate(minKey, cursor);
	  //read forward from the minKey position, and print the tuples satisfying requirement
	  while (tree.readForward(cursor, key, rid) == 0)
	  {
		  // read the tuple, if only count required, not need to read
		  if (attr != 4)
		  {
			  if ((rc = rf.read(rid, key, value)) < 0) {
				  fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
				  goto exit_select;
			  }
		  }

		  // check the conditions on the tuple
		  for (unsigned i = 0; i < cond.size(); i++) {
			  // compute the difference between the tuple value and the condition value
			  switch (cond[i].attr) {
			  case 1:
				  diff = key - atoi(cond[i].value);
				  break;
			  case 2:
				  diff = strcmp(value.c_str(), cond[i].value);
				  break;
			  }

			  // skip the tuple if any condition is not met
			  switch (cond[i].comp) {
			  case SelCond::EQ:
				  if (diff != 0) 
					  if (cond[i].attr == 2) goto next_tuple2;
					  else goto exit_select;
				  break;
			  case SelCond::NE:
				  if (diff == 0) goto next_tuple2;
				  break;
			  case SelCond::GT:
				  if (diff <= 0) goto next_tuple2;
				  break;
			  case SelCond::LT:
				  if (diff >= 0) 
					  if (cond[i].attr == 2) goto next_tuple2;
					  else goto exit_select;
				  break;
			  case SelCond::GE:
				  if (diff < 0) goto next_tuple2;
				  break;
			  case SelCond::LE:
				  if (diff > 0)
					  if (cond[i].attr == 2) goto next_tuple2;
					  else goto exit_select;
				  break;
			  }
		  }

		  // the condition is met for the tuple. 
		  // increase matching tuple counter
		  count++;

		  // print the tuple 
		  switch (attr) {
		  case 1:  // SELECT key
			  fprintf(stdout, "%d\n", key);
			  break;
		  case 2:  // SELECT value
			  fprintf(stdout, "%s\n", value.c_str());
			  break;
		  case 3:  // SELECT *
			  fprintf(stdout, "%d '%s'\n", key, value.c_str());
			  break;
		  }

		  // move to the next tuple
	  next_tuple2:
		  continue;
	  }
  }
  else
  {
	  while (rid < rf.endRid()) {
		  // read the tuple
		  if ((rc = rf.read(rid, key, value)) < 0) {
			  fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
			  goto exit_select;
		  }

		  // check the conditions on the tuple
		  for (unsigned i = 0; i < cond.size(); i++) {
			  // compute the difference between the tuple value and the condition value
			  switch (cond[i].attr) {
			  case 1:
				  diff = key - atoi(cond[i].value);
				  break;
			  case 2:
				  diff = strcmp(value.c_str(), cond[i].value);
				  break;
			  }

			  // skip the tuple if any condition is not met
			  switch (cond[i].comp) {
			  case SelCond::EQ:
				  if (diff != 0) goto next_tuple;
				  break;
			  case SelCond::NE:
				  if (diff == 0) goto next_tuple;
				  break;
			  case SelCond::GT:
				  if (diff <= 0) goto next_tuple;
				  break;
			  case SelCond::LT:
				  if (diff >= 0) goto next_tuple;
				  break;
			  case SelCond::GE:
				  if (diff < 0) goto next_tuple;
				  break;
			  case SelCond::LE:
				  if (diff > 0) goto next_tuple;
				  break;
			  }
		  }

		  // the condition is met for the tuple. 
		  // increase matching tuple counter
		  count++;

		  // print the tuple 
		  switch (attr) {
		  case 1:  // SELECT key
			  fprintf(stdout, "%d\n", key);
			  break;
		  case 2:  // SELECT value
			  fprintf(stdout, "%s\n", value.c_str());
			  break;
		  case 3:  // SELECT *
			  fprintf(stdout, "%d '%s'\n", key, value.c_str());
			  break;
		  }

		  // move to the next tuple
	  next_tuple:
		  ++rid;
	  }
  }
  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* your code here */
    fstream fin;
    fin.open(loadfile.c_str(),ios::in);
    if(!fin) {
        fprintf(stderr, "Error: loadfile %s does not exist\n", loadfile.c_str());
        return -1;
    }
    string line;
    string tablename=table+".tbl";
    RC rc;
    RecordFile rf;
    //SqlEngine se;
    if ((rc = rf.open(table + ".tbl", 'w')) < 0) {
        fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
        return rc;
    }
	BTreeIndex tree;
	if (index)
	{
		if (tree.open(table + ".idx", 'w') < 0) return -1;
	}

    int key;
    string value;
    RecordId id;
    while (getline(fin,line)) {
        if (SqlEngine::parseLoadLine(line,key,value)) return -1;
        //printf("%d, %s",key,value.c_str());
        if (rf.append(key,value,id)) return -1;
		else if (index) tree.insert(key, id);
    }
	if (index) tree.close();
    fin.close();
    rf.close();
    return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
