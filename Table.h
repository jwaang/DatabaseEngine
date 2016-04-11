#pragma once

#include "Tuple.h"
#include "Condition.h"


class Table
{
private:
  string name;
  Tuple template_tuple;
public:
  vector<Tuple> tuples;
  Table(const Table& t);
  Table(string n, Tuple template_t);
  Table(string n, Tuple template_t, vector<Tuple> t);

  void insert(Tuple new_tuple);          //Inserts a new Tuple into a Table
  bool is_Union_Compatible(Table table); //Checks another table for union compatibility
  Table operator+(Table table);          //Returns a Table that is the union of two Tables
  void remove(Tuple tuple);              //Deletes a Tuple in a Table
  Table operator-(Table table);          //returns the difference between two Tables
  Table operator*(Table table);          //Returns a table that is the cartesian product of two tables
  bool is_empty();                       //returns true if the table is empty and false otherwise

  string get_attr_value(string name);                //assumes table only has one tupe, returns value of given attribute
  
  string get_Name() const;                     //Returns Table name
  void set_Name(string new_name);             //Sets Table name
  vector<Tuple> get_Tuples() const;            //Returns the Tuples vector
  Tuple get_Template_Tuple() const;            //returns template tuple vector
  void set_Template_Tuple(Tuple template_tup);

  void show(); //For debugging

  vector<Attribute> get_Column(string attr_name); //Returns the column corresponding
                                                  //to that attribute name in the table

  // returns 0 if the attribute is successfully changed, 1 for failure
  void replace_Attribute(string attr_name, Attribute attr, int tuple_index);
};
