#pragma once

#include "Tuple.h"
#include "Attribute.h"
#include <string>
#include <vector>

using namespace std;

struct Condition_Conj;

struct Condition_Sing
{
public:
  string attribute;
  string compare_operator;
  string value;
  
  Condition_Sing();
  Condition_Sing(string a, string c, string v);

  bool eval_condition(Tuple tuple);
};

struct Condition
{
  public:
    Condition_Sing* cond_sing;
    Condition_Conj* cond_conj;
    bool is_conj;
    
    Condition();
    Condition(Condition_Sing cond);
    Condition(Condition_Conj cond);
    
    bool eval_condition(Tuple tuple);
};

struct Condition_Conj
{
public:
  Condition con1;
  Condition con2;
  string conjunction;
  
  Condition_Conj();
  Condition_Conj(Condition c1, Condition c2, string conj);

  bool eval_condition(Tuple tuple);
};

