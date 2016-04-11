#include "DB_Set.h"
#include "DB_Engine.h"
#include "Relation_Ops.h"
#include "Parser.h"
#include <fstream>
#include <iostream>
#include <iomanip>

bool DB_Engine::exit = true;

Table* DB_Engine::open_relation(string filename)
{
	ifstream fs;
	ofstream ofs;
	Table* old_ptr;
	Table* new_ptr;
	
	fs.open(filename);
	if(!fs.is_open())
	{
		cerr << "File " << filename << " does not exit" << endl;
		return nullptr;
	}
	string tmp, file;
	while(fs)
	{
		if(tmp=="open")
		{
			cerr<< "File " << filename << " is already open" <<endl;
			return nullptr;
		}
		else
		{
			getline(fs, tmp);
			file.append(tmp);
		}
	}

	fs.close();
	
	DB_Set p;
	p.input(file);
	
	if(p.is_empty())
	{
		cout<<"file not opened. could not read.\n";
		return nullptr;
	}
	
	file.append("\nopen\n");
	
	ofs.open(filename);
	ofs<<file;
	ofs.close();
	
	DB_Engine::set_exit(false);
	
	old_ptr = p.get_last();
	new_ptr = new Table(*old_ptr);
	
	
	return new_ptr;
}
void DB_Engine::close_relation(Table* relation, string filename)
//write to file without word "open" at end of file
{  
  ofstream file;  
  file.open(filename.c_str());
  
  save_template(relation, file);
  
  //save each tuple
  for(int i=0; i<relation->tuples.size(); ++i)
  {
    save_tuple(relation, relation->tuples[i], file);
  }
  
  file<<"EXIT;";
  
  delete relation;  //take closed relation out of memory
}  


void DB_Engine::save_relation(Table* relation, string filename)
//write to file with word "open" at end of file
{
  ofstream file;  
  file.open(filename.c_str());
  
  save_template(relation, file);
  
  //save each tuple
  for(int i=0; i<relation->tuples.size(); ++i)
  {
    save_tuple(relation, relation->tuples[i], file);
  }
  
  file<<"EXIT;\n";
  file<<"open\n"; //tells engine that relation is already open

}

void DB_Engine::exit_interp()
{
  exit = true;
}

void DB_Engine::show(Table relation)
{
	vector<Attribute> templ_attributes = relation.get_Template_Tuple().get_Attributes();
	vector<Attribute> current_tuple;
	bool first = true;
	
	cout<<"\n------------------------------------------------------------\n";
	cout<<"Relation: "<< relation.get_Name()<<"\n\n";
	
	//print column headers
	cout<<"(";
	for(int i=0; i<templ_attributes.size(); ++i)
	{
		if(!first) cout<<", ";
		cout<< templ_attributes[i].get_Name();
		first = false;
	}
	cout<<")\n\n";
	first = true;

	//print tuples
	for(int i=0; i<relation.tuples.size(); ++i)
	{
		current_tuple = relation.tuples[i].get_Attributes();
		
		cout<<"(";
		for(int j=0; j<current_tuple.size(); ++j)
		{
			if(!first) cout<< ", ";
			if(current_tuple[j].is_Varchar())
			{
				cout<<current_tuple[j].get_String_Value();
			}
			else
			{
				cout<<current_tuple[j].get_Int_Value();
			}
			first = false;
		}
		cout<<")\n";
		first = true;
	}
	cout<<"\n------------------------------------------------------------\n";
	cout<<"\n";
}

Table* DB_Engine::create_relation(string name, Tuple tuple_template)
{
  Table* relation = new Table(name, tuple_template);
  return relation;
}

void DB_Engine::update(Table* relation, vector<string> attribute_names, vector<string> values, Condition cond)
{
	Table update_list = *Relation_Ops::select(cond, *relation);		//table of tuples to be updated
	vector<Tuple> tuples = update_list.tuples;						//list of tuples to be updated
	
	//check to make sure number of update values matches number of attributes to be updated
	if(values.size()!=values.size())
	{
		cerr<<"Different number of attribtues and values in update func";
	}
	
	//replace necessary attributes
	for(int i=0; i<relation->tuples.size(); ++i)
	{
		for(int j=0; j<tuples.size(); ++j)
		{
			if(relation->tuples[i]==tuples[j])
			{
				for(int k=0; k<values.size(); ++k)
				{
       
          relation->tuples[i].replace_Attribute(attribute_names[k], values[k]);
          
				}
			}
		}
	}
	
	
}

void DB_Engine::insert_tuple(Table* relation, Tuple tuple)
{
	relation->insert(tuple);
}

void DB_Engine::insert_relation(Table* relation, Table new_relation)
{
	//insert each tuple of new_relation into relation
	for(int i=0; i<new_relation.tuples.size(); ++i)
	{
		relation->insert(new_relation.tuples[i]);
	}
	relation->show();
}

void DB_Engine::remove_query(Table* relation, Condition cond)
{
	Table update_list = *Relation_Ops::select(cond, *relation);		//table of tuples to be updated
	vector<Tuple> tuples = update_list.tuples;						//list of tuples to be updated
	
	//remove each tuple in tuples from relation
	for(int i=0; i<tuples.size(); ++i)
	{
		relation->remove(tuples[i]);
	}
}

/***********************************************************************************
Helper functions
************************************************************************************/

void DB_Engine::save_template(Table* relation, ofstream& file)
{
  vector<Attribute> primary_key;
  vector<Tuple> tuples = relation->tuples;
  vector<Attribute> tuple_template = relation->get_Template_Tuple().get_Attributes();
  
  
  file<<"CREATE TABLE " << relation->get_Name() << " (";
  
  //print list of Attributes
  for(int i=0; i<tuple_template.size(); ++i)
  {
    if(i!=0)
    {
      file<<", ";
    }
    
    if(tuple_template[i].is_Primary())
    {
      primary_key.push_back(tuple_template[i]);
    }
    file<< tuple_template[i].get_Name() << " ";

    if(tuple_template[i].is_Int())
    {
      file<< "INTEGER";
    }
    else if(tuple_template[i].is_Varchar())
    {
      file<< "VARCHAR(" << tuple_template[i].get_Length() << ")";
    }
    else
    {
      cerr<<"no valid attribute type";
    }
  }  
    
  file<<") PRIMARY KEY (";
  
  //print list of primary keys
  if(primary_key.size() == 0)
  {
    cerr<<"No primary keys";
  }
  else
  {
    for(int i=0; i<primary_key.size(); ++i)
    {
      if(i!=0)
      {
        file<<", ";
      }
      file<< primary_key[i].get_Name();
    }
    file<<");\n";
  }   
}

void DB_Engine::save_tuple(Table* relation, Tuple tuple, ofstream& file)
{
  vector<Attribute> attributes = tuple.get_Attributes();
  
  file<<"INSERT INTO "<< relation->get_Name() << " VALUES FROM (";
  
  //print list of attribute values
  for(int i=0; i<attributes.size(); ++i)
  {
    if(i!=0)
    {
      file<<", ";
    }
    
    if(attributes[i].is_Varchar())
    {
      file<<"\""<<attributes[i].get_String_Value()<<"\"";
    }
    else
    {
      file<<attributes[i].get_Int_Value();
    }
  }
  file<<");\n";
} 