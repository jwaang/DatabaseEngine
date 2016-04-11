#pragma once

#include "Table.h"
#include "Attribute.h"
#include "Condition.h"
#include <vector>



class DB_Engine
{
private:
  static bool exit; 													//used to exit program in main

public:
  //DB Core functions
  static Table* open_relation(string filename);							//loads relation from file to mem
  static void close_relation(Table* relation, string filename); 		//saves and closes
  static void save_relation(Table* relation, string filename);			//saves and leaves relation open
  static void exit_interp();
  static void show(Table relation);										//prints relation to screen
  static Table* create_relation(string name, Tuple tuple_template);		//creates new relation from template tuple
  static void update(Table* relation, vector<string> attribute_names, vector<string> values, Condition cond);
																		//makes update for all tuples that meet cond
  static void insert_tuple(Table* relation, Tuple tuple);				//inserts tuple into relation
  static void insert_relation(Table* relation, Table new_relation);		//inserts copy of relation into relation
  static void remove_query(Table* relation, Condition cond);			//removes all tuples that meet a certain condition
  
  //other useful functions
  static bool get_exit() {return exit;}
  static void set_exit(bool b) {exit = b;}

private:
  //helper functions
  static void save_template(Table* relation, ofstream& file);  			//prints first line of db file
  static void save_tuple(Table* relation, Tuple tuple, ofstream& file); //prints a tuple to db file
};