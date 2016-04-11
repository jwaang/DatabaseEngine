#include "Tuple.h"

Tuple::Tuple() : attributes() {}

Tuple::Tuple(vector<Attribute> a) : attributes(a) {}

int Tuple::get_Size()
{
  return attributes.size();
}

vector<Attribute> Tuple::get_Attributes()
{
  return this->attributes;
}

bool Tuple::operator==(Tuple tuple)
{
  return this->attributes == tuple.get_Attributes();
}

bool Tuple::operator!=(Tuple tuple)
{
  return !(*this == tuple);
}

Tuple Tuple::operator+(Tuple t)
{
  Tuple new_tuple = *this;
  for(int i = 0; i < t.get_Attributes().size(); i++) {
    new_tuple.attributes.push_back(t.get_Attributes()[i]);
  }
  return new_tuple;
}

void Tuple::show() {
  for(int i = 0; i < this->attributes.size(); i++) {
    this->attributes[i].show();
    cout << "\t";
  }
}

void Tuple::replace_Attribute(string attr_name, string attr)
{
  for(int i = 0; i < this->attributes.size(); i++) {
    if(this->attributes[i].get_Name() == attr_name) {
        Attribute temp = attributes[i];
        if(temp.is_Int())
        {
          temp.set_Int_Value(atoi(attr.c_str()));
        }
        else
        {
          temp.set_String_Value(attr);
        }        
        this->attributes[i] = temp;
    }
  }
}

void Tuple::replace_Attribute(string attr_name, Attribute attr)
{
  for(int i = 0; i < this->attributes.size(); i++) {
    if(this->attributes[i].get_Name() == attr_name) {
        this->attributes[i] = attr;
    }
  }
}

bool Tuple::is_Primarily_Equal(Tuple t)
{
  bool flag=true;
  vector<Attribute> rhs = this->get_Attributes();
  vector<Attribute> lhs = t.get_Attributes();

  if(rhs.size() != lhs.size()) {
    return false;
  }

  for(unsigned int i = 0; i < this->get_Size(); ++i) {
    if( this->get_Attributes()[i].is_Primary() &&
      !(this->get_Attributes()[i].is_Compatible(t.get_Attributes()[i])) ) {
         flag = false;
    }
  }

  return flag;
}

bool Tuple::is_Compatible(Tuple t)
{
  vector<Attribute> rhs = this->get_Attributes();
  vector<Attribute> lhs = t.get_Attributes();

  if(rhs.size() != lhs.size()) {
    return false;
  }

  for(unsigned int i = 0; i < rhs.size(); ++i) {
	if(!rhs[i].is_Compatible(lhs[i])) {
	  return false;
	}
  }

  return true;
}

bool Tuple::equal_On_Keys(Tuple t, vector<Attribute> keys)
{
  for(int i = 0; i < keys.size(); i++){
    if( *(get_Attribute(keys[i].get_Name())) != *(t.get_Attribute(keys[i].get_Name())) )
      return false;
    }

    return true;
}

bool Tuple::is_attribute(string s)
{
	for(int i =0; i<attributes.size(); ++i)
	{
		if (attributes[i].get_Name() == s) return true;
	}
	
	return false;
	
}

Attribute* Tuple::get_Attribute(string attribute_name)
{
  for(int i = 0; i < attributes.size(); i++){
	Attribute& a = attributes[i];
    if(a.get_Name() == attribute_name)
      return &a;
  }

  return new Attribute("null",false,"empty_attribute",1);
}

Attribute Tuple::get_Attribute_Copy(string attribute_name)
{
  for(int i = 0; i < attributes.size(); i++){
	Attribute a = attributes[i];
    if(a.get_Name() == attribute_name)
      return a;
  }

  return Attribute("null",false,"empty_attribute",1);
}

