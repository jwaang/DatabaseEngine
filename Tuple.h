#pragma once

#include "Attribute.h"
#include <stdlib.h>

class Tuple
{
private:
  vector<Attribute> attributes;
public:
  Tuple();                      // used for empty tuples
  Tuple(vector<Attribute> a);   // used to initialize full tuple
  bool operator==(Tuple t);
  bool operator!=(Tuple t);
  Tuple operator+(Tuple t);
  void show(); 
  vector<Attribute> get_Attributes();
  int get_Size();
  void replace_Attribute(string attr_name, string attr);
  void replace_Attribute(string attr_name, Attribute attr);
  bool is_Primarily_Equal(Tuple t);
  bool is_Compatible(Tuple t);
  bool equal_On_Keys(Tuple t, vector<Attribute> keys);
  bool is_attribute(string s);
  Attribute* get_Attribute(string attribute_name);
  Attribute get_Attribute_Copy(string attribute_name);
};

//c++ 11 compiler not working for me, which means auto can't be used.

/*inline ostream& operator<<(ostream& os, Tuple& t)
{
  os << "(";

  vector<Attribute> attributes = t.get_Attributes();

  for(auto i = attributes.begin(); i != attributes.end(); ++i) {
    os << *i;
    if(attributes.cend() - i != 1) {
      os << ", ";
    }
  }
  
  os << ");";

  return os;
}*/
