#include "Condition.h"

/******************************************************************
Condition_Sing
******************************************************************/

Condition_Sing::Condition_Sing(string a, string c, string v) : 
  attribute(a), compare_operator(c), value(v) {}

Condition_Sing::Condition_Sing() :
  attribute(""), compare_operator(""), value("") {}
  
bool Condition_Sing::eval_condition(Tuple tuple)
{
  Attribute attr = *(tuple.get_Attribute(attribute));
  int compare_int = atoi(value.c_str());
  string compare_str = value;

  if(tuple.is_attribute(value))
  {
	Attribute second = *(tuple.get_Attribute(value));
	if(attr.is_Int())
	{
		compare_int = second.get_Int_Value();
	}
	else
	{
		compare_str = second.get_String_Value();
	}
  }
  if(attr.is_Int())
  {
    int attr_int = attr.get_Int_Value();
    
    if(compare_operator == "==")
    {
      return (attr_int == compare_int);
    }
    else if(compare_operator == "!=")
    {
      return (attr_int != compare_int);
    }
    else if(compare_operator == "<")
    {
      return(attr_int < compare_int);
    }
    else if(compare_operator == "<=")
    {
      return(attr_int <= compare_int);
    }
    else if(compare_operator == ">")
    {
      return (attr_int > compare_int);
    }
    else if(compare_operator == ">=")
    {
      return (attr_int >= compare_int);
    }
    else
    {
      cerr << "invalid compareator in condition";
      return false;
    }
  }
  else
  {
    string attr_str = attr.get_String_Value();
    
    if(compare_operator == "==")
    {
      return (attr_str == compare_str);
    }
    else if(compare_operator == "!=")
    {
      return (attr_str != compare_str);
    }
    else if(compare_operator == "<")
    {
      return(attr_str < compare_str);
    }
    else if(compare_operator == "<=")
    {
      return(attr_str <= compare_str);
    }
    else if(compare_operator == ">")
    {
      return (attr_str > compare_str);
    }
    else if(compare_operator == ">=")
    {
      return (attr_str >= compare_str);
    }
    else
    {
      cerr << "invalid compareator in condition";
      return false;
    }
  }
}

/******************************************************************
Condition_Conj
******************************************************************/

Condition_Conj::Condition_Conj(Condition c1, Condition c2, string conj) :
  con1(c1), con2(c2), conjunction(conj) {}
  
Condition_Conj::Condition_Conj() :
  conjunction("") {}

bool Condition_Conj::eval_condition(Tuple tuple)
{
  //check conjunction and eval appropriately
  if(conjunction == "||")
  {
    return (con1.eval_condition(tuple) || con2.eval_condition(tuple));
  }
  else if(conjunction == "&&")
  {
    return (con1.eval_condition(tuple) && con2.eval_condition(tuple));
  }
  else
  {
	cerr<<"condition has improper conjunction " << conjunction << "\n";
    return false;
  }
}

/******************************************************************
Condition
******************************************************************/

Condition::Condition() :
  is_conj(false) {}

Condition::Condition(Condition_Sing cond) :
  is_conj(false) {
	cond_sing = new Condition_Sing(cond.attribute, cond.compare_operator, cond.value);
	cond_conj = nullptr;
  }
  
Condition::Condition(Condition_Conj cond) :
  is_conj(true) {
	cond_sing = nullptr;
	cond_conj = new Condition_Conj(cond.con1, cond.con2, cond.conjunction);
  }
  
bool Condition::eval_condition(Tuple tuple)
{
  if(is_conj)
  {
	return (cond_conj->eval_condition(tuple));
  }
  else
  {
    return (cond_sing->eval_condition(tuple));
  }
}
