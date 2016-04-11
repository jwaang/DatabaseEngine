#include "Table.h"

#include <algorithm>

Table::Table(const Table& t) : name(t.get_Name()), template_tuple(t.get_Template_Tuple()), tuples(t.get_Tuples()) { }

Table::Table(string n, Tuple template_t) : name(n), template_tuple(template_t), tuples() { }

Table::Table(string n, Tuple template_t, vector<Tuple> t) : name(n), template_tuple(template_t), tuples(t) { }

void Table::insert(Tuple new_tuple)
{
  /*for(int i; i<tuples.size(); ++i){
    if(!tuples[i].is_Primarily_Equal(new_tuple)) {
	  return;
	  }
  }*/

  this->tuples.push_back(new_tuple);
}

bool Table::is_Union_Compatible(Table table)
{
  if(this->get_Tuples().size() == 0 || table.get_Tuples().size() == 0)
    return true;

  Tuple lhs = this->get_Tuples().front();
  Tuple rhs = table.get_Tuples().front();

  if(lhs.is_Compatible(rhs)) {
    return true;
  }

  return false;
}

Table Table::operator+(Table table)
{
  Table new_table = *this;
  new_table.set_Name(this->get_Name() + " + " + table.get_Name());

  if(this->is_Union_Compatible(table)) {
    vector<Tuple> tuples = new_table.get_Tuples();
    bool found = false; //flag
    vector<Tuple> tuples2 = table.get_Tuples();
    for(int i=0; i<tuples2.size(); ++i)
    {
      found = false;
      for(int j=0; j<tuples.size(); ++j)
      {
        if(tuples2[i]==tuples[j])
        {
          found = true;
          break;  //nothing happens, tuple is already in new_table
        }          
      }
      if(!found)
      {
        new_table.insert(tuples2[i]);
      }
    }
  }
  return new_table;
}

void Table::remove(Tuple tuple)
{
  bool found = false;
  for(int i=0; i<tuples.size(); ++i) {
    if(tuples[i] == tuple)
    {
      found = true;
    }
    else if(found)
    {
      tuples[i-1] = tuples[i];
    }
  }
  tuples.pop_back();  
}

Table Table::operator-(Table table)
{
  Table new_table = *this;
  new_table.set_Name(this->get_Name() + " - " + table.get_Name());

  if(this->is_Union_Compatible(table)) {
  
    vector<Tuple> tuples = new_table.get_Tuples();
    vector<Tuple> tuples2 = table.get_Tuples();
    
    for(int i=0; i<tuples2.size(); ++i)
    {
      for(int j=0; j<tuples.size(); ++j)
      {
        if(tuples2[i]==tuples[j])
        {
          new_table.remove(tuples[j]);
          break;
        }          
      }
    }
  }
  return new_table;
}

Table Table::operator*(Table table)
{
  vector<Tuple> new_tuples = vector<Tuple>();
  vector<Tuple> empty = vector<Tuple>();
  
  if(this->is_empty() || table.is_empty())
  {
	new_tuples.push_back(template_tuple + table.get_Template_Tuple());
	return Table(this->get_Name() + "*" + table.get_Name(), new_tuples[0]);
  }

 
  for(int i=0; i<tuples.size(); ++i)
  {
    for(int j=0; j<table.get_Tuples().size(); ++j)
    {
      new_tuples.push_back(tuples[i] + table.get_Tuples()[j]);
    }
  }

  return Table( this->get_Name() + "*" + table.get_Name(), new_tuples[0], new_tuples);
}

bool Table::is_empty()
{
  if(tuples.size()==0)
  {
    return true;
  }
  else
  {
    return false;
  }
}

string Table::get_attr_value(string name)
{
  //check to see if only one tuple
  if(tuples.size()<=0 || tuples.size()>1)
  {
    cerr<<"Invalid table in get_attr_value\n";
    return "";
  }
  
  Tuple tuple = tuples[0];
  Attribute attr = tuple.get_Attribute_Copy(name);
  if(attr.get_Name() == "empty_attribute")
  {
    cerr<<"Invalid attribute name\n";
    return "";
  }
  
  if(attr.is_Int())
  {
    return to_string(attr.get_Int_Value());
  }
  else
  {
    return attr.get_String_Value();
  }
}

string Table::get_Name() const
{
  return name;
}

void Table::set_Name(string new_name)
{
  this->name = new_name;
}

vector<Tuple> Table::get_Tuples() const
{
  return this->tuples;
}

Tuple Table::get_Template_Tuple() const
{
  return template_tuple;
}

void Table::set_Template_Tuple(Tuple t) {
	this->template_tuple = t;
}

void Table::show()
{
  cout << "Table: " << name << "\n";
  for(unsigned int i = 0; i < this->tuples.size(); ++i) {
    cout << i << " Contents: ";
    tuples[i].show();
    cout << "\n";
  }
}

vector<Attribute> Table::get_Column(string attr_name)
{
  vector<Attribute> column;

  for(int i=0; i<tuples.size(); ++i)
  {
    vector<Attribute> attributes = tuples[i].get_Attributes();
    for(int j=0; j<tuples.size(); ++j)
    {
      if(attributes[j].get_Name() == attr_name)
      {
        column.push_back(attributes[j]);
      }
    }
  }
  return column;
}

//Replaces an attribute of the tuple specified via index
void Table::replace_Attribute(string attr_name, Attribute attr, int tuple_index)
{
  this->tuples[tuple_index].replace_Attribute(attr_name, attr);
}