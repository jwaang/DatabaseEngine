#include<iostream>
#include<vector>
#include "DB_Engine.h"
#include "Tuple.h"
#include "Table.h"
#include "Attribute.h"
#include "Relation_Ops.h"
#include "Condition.h"

using namespace std;

int main()
{
  //Make relation
  Attribute name("", true, "name", 10);
  Attribute mascot("", false, "mascot", 10);
  Attribute conquests(-99, false, "conquests");
  vector<Attribute> templ_attr;
  templ_attr.push_back(name);
  templ_attr.push_back(mascot);
  templ_attr.push_back(conquests);
  Tuple templ(templ_attr);
  Table* relation = new Table("Pirates", templ);
  
  name.set_String_Value("Bob");
  mascot.set_String_Value("parrot");
  conquests.set_Int_Value(29);
  
  templ_attr[0] = name;
  templ_attr[1] = mascot;
  templ_attr[2] = conquests;
  
  templ = Tuple(templ_attr);
  
  relation->insert(templ);
  
  name.set_String_Value("Joe");
  mascot.set_String_Value("skull");
  conquests.set_Int_Value(2);
  
  templ_attr[0] = name;
  templ_attr[1] = mascot;
  templ_attr[2] = conquests;
  
  templ = Tuple(templ_attr);
  
  relation->insert(templ);
  
  name.set_String_Value("Davy");
  mascot.set_String_Value("octopus");
  conquests.set_Int_Value(345);
  
  templ_attr[0] = name;
  templ_attr[1] = mascot;
  templ_attr[2] = conquests;
  
  templ = Tuple(templ_attr);
  
  relation->insert(templ);
  
  //All prints use show function
  cout<<"Print relation\n\n";
  DB_Engine::show(*relation);
  //test save
  DB_Engine::save_relation(relation, "relation_save.db");
  
  //make relation 2, test insert for relation
  cout<<"\nTesting insert relation\nshowing original relation2\n\n";
  Table* relation2 = DB_Engine::create_relation("Pirates2",templ);
  DB_Engine::show(*relation2);
  cout<<"relation2 after inserting relation\n\n";
  DB_Engine::insert_relation(relation2, *relation);
  DB_Engine::show(*relation2);
  
  //make condition for a small number of conquests
  Condition_Sing few_conquest_sing("conquests", "<=", "30");
  Condition few_conquest(few_conquest_sing);
  
  //test select
  cout<<"\nselecting pirates with conquests less than 30 from relation2\n\n";
  Table* query = Relation_Ops::select(few_conquest, *relation2);
  DB_Engine::show(*query);
  
  //test remove
  cout<<"\nremoving pirates with conquests less than 30 from relation2\n\n";
  DB_Engine::remove_query(relation2, few_conquest);
  DB_Engine::show(*relation2);
  
  //test project
  cout<<"\nprojecting name and mascot from relation\n\n";
  vector<string> attributes;
  attributes.push_back("name");
  attributes.push_back("mascot");
  delete query;
  query = Relation_Ops::project(attributes, *relation);
  Table* query2 = Relation_Ops::project(attributes, *relation2);
  
  DB_Engine::show(*query);
  cout<<"\nprojecting name and mascot from relation2\n\n";
  DB_Engine::show(*query2);
  
  //test rename
  cout<<"\nrenaming name and mascot in relation\n\n";
  vector<string> new_names;
  new_names.push_back("Pirate Name");
  new_names.push_back("Flag");
  Relation_Ops::rename(attributes, new_names, relation);
  DB_Engine::show(*relation);
  
  //reset relation
  delete relation;
  relation = new Table(*relation2);
  relation->set_Name("Pirates1");
  
  name.set_String_Value("Steve");
  mascot.set_String_Value("fish");
  conquests.set_Int_Value(12);
  
  templ_attr[0] = name;
  templ_attr[1] = mascot;
  templ_attr[2] = conquests;
  
  templ = Tuple(templ_attr);
  
  //test insert
  cout<<"\nshowing reset relation\n\n";
  DB_Engine::show(*relation);
  DB_Engine::insert_tuple(relation, templ);
  cout<<"\nshowing relation with Steve inserted\n\n";
  DB_Engine::show(*relation);
  
  name.set_String_Value("Jerry");
  mascot.set_String_Value("cannon");
  conquests.set_Int_Value(90);
  
  templ_attr[0] = name;
  templ_attr[1] = mascot;
  templ_attr[2] = conquests;
  
  templ = Tuple(templ_attr);
  
  DB_Engine::insert_tuple(relation2, templ);
  
  //test union
  delete query;
  query = Relation_Ops::set_union(*relation, *relation2);
  cout<<"\nunion of relation and relation2\n\n";
  DB_Engine::show(*relation);
  DB_Engine::show(*relation2);
  DB_Engine::show(*query);
  
  //test set difference
  cout<<"\ndifference of last query and relation\n\n";
  delete query2;
  query2 = Relation_Ops::set_diff(*query, *relation);
  
  DB_Engine::show(*query);
  DB_Engine::show(*relation);
  DB_Engine::show(*query2);
  
  //test cross product
  cout<<"\ncross product of union query and relation\n\n";
  delete query2;
  query2 = Relation_Ops::cross_prod(*query, *relation);
  
  DB_Engine::show(*query);
  DB_Engine::show(*relation);
  DB_Engine::show(*query2);
  
  //reset relation
  name.set_String_Value("Bob");
  mascot.set_String_Value("parrot");
  conquests.set_Int_Value(29);
  
  templ_attr[0] = name;
  templ_attr[1] = mascot;
  templ_attr[2] = conquests;
  
  templ = Tuple(templ_attr);
  
  delete relation;
  relation = new Table("Pirates3", templ);
  
  relation->insert(templ);
  
  name.set_String_Value("Joe");
  mascot.set_String_Value("skull");
  conquests.set_Int_Value(2);
  
  templ_attr[0] = name;
  templ_attr[1] = mascot;
  templ_attr[2] = conquests;
  
  templ = Tuple(templ_attr);
  
  relation->insert(templ);
  
  name.set_String_Value("Davy");
  mascot.set_String_Value("octopus");
  conquests.set_Int_Value(345);
  
  templ_attr[0] = name;
  templ_attr[1] = mascot;
  templ_attr[2] = conquests;
  
  templ = Tuple(templ_attr);
  
  relation->insert(templ);
  
  cout<<"\nshow reset relation\n\n";
  DB_Engine::show(*relation);
  
  //test update
  attributes[0] = "conquests";
  vector<string> new_vals;
  new_vals.push_back("0");
  new_vals.push_back("sad face");
  
  cout<<"\nUpdating pirates with few_conquests in relation\n\n";
  DB_Engine::update(relation, attributes, new_vals, few_conquest);
  DB_Engine::show(*relation);
  
  //test close
  DB_Engine::close_relation(relation, "relation_close.db");
} 