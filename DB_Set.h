#pragma once

#include "Parser.h"
#include "DB_Engine.h"
#include "Relation_Ops.h"
#include "Table.h"
#include "Attribute.h"
#include "Condition.h"
#include <vector>
#include <string>
using namespace std;

class DB_Set
{
public:
	void input(string s);
	Table* lookup(string s);
	int lookup(Table*);
	Table* get_last() {return tables[tables.size()-1];}
	bool is_empty() {return (tables.size()==0);}
  
    DB_Set();
	//~DB_Set();
private:
	Parser p;
	vector<Table*> tables;

	void interpret(token t);
	void query_interpret(token& t, vector<Table*> r);
	Table* expr_interpret(token& t, vector<Table*> r);
	Table* atomic_expr_interpret(token& t, vector<Table*> r);
	Table* selection_interpret(token& t, vector<Table*> r);
	Table* projection_interpret(token& t, vector<Table*> r);
	Table* renaming_interpret(token& t, vector<Table*> r);
	Table* union_interpret(token& t, vector<Table*> r);
	Table* product_interpret(token& t, vector<Table*> r);
	Table* difference_interpret(token& t, vector<Table*> r);
	Condition condition_interpret(token& t, vector<Table*> r);
	Condition conjunction_interpret(token& t, vector<Table*> r);
	Condition comparison_interpret(token& t, vector<Table*> r);
	void command_interpret(token& t, vector<Table*> r);
	void open_cmd_interpret(token& t, vector<Table*> r);
	void close_cmd_interpret(token& t, vector<Table*> r);
	void save_cmd_interpret(token& t, vector<Table*> r);
	void exit_cmd_interpret(token& t, vector<Table*> r);
	void show_cmd_interpret(token& t, vector<Table*> r);
	void create_cmd_interpret(token& t, vector<Table*> r);
	void update_cmd_interpret(token& t, vector<Table*> r);
	void insert_cmd_interpret(token& t, vector<Table*> r);
	void delete_cmd_interpret(token& t, vector<Table*> r);
};