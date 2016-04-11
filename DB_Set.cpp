#include "DB_Set.h"
#include <sstream>

DB_Set::DB_Set() : tables(vector<Table*>()) {}

void DB_Set::input(string s)
{
	token t = p.parse(s);
	interpret(t);
}

Table* DB_Set::lookup(string s)
{
	Table* ptr = nullptr;
	for(int i = 0; i < tables.size(); ++i)
	{
		if((*(tables[i])).get_Name() == s)
		{
			ptr = tables[i];
			break;
		}
	}
	return ptr;
}

int DB_Set::lookup(Table* t)
{
	int r=-1;
	for(int i = 0; i < tables.size(); ++i)
	{
		if(tables[i] == t)
		{
			r = i;
			break;
		}
	}
	return r;
}

void DB_Set::interpret(token t)
{
	token tmp = t;
	vector<Table*> relations;
	while(tmp.get_type() == program)
	{
		if(tmp[0].get_type() == query)
		{
			query_interpret(tmp[0], relations);
		}
		else if(tmp[0].get_type() == command)
		{
			command_interpret(tmp[0], relations);
		}
		else if(tmp[0].get_type() == fail)
		{
			cout << "Invalid command (make sure you have semicolon)." << endl;
		}
		tmp = tmp[1];
	}
}

void DB_Set::query_interpret(token& t, vector<Table*> r)
{
	Table* e = expr_interpret(t[1], r);
	if(e != nullptr) 
	{
		e->set_Name(t[0].get_value());
		tables.push_back(e);
	}
}

Table* DB_Set::expr_interpret(token& t, vector<Table*> r)
{
	if(t[0].get_type() == atomic_expr) return atomic_expr_interpret(t[0], r);
	else if(t[0].get_type() == selection) return selection_interpret(t[0], r);
	else if(t[0].get_type() == projection) return projection_interpret(t[0], r);
	else if(t[0].get_type() == renaming) return renaming_interpret(t[0], r);
	else if(t[0].get_type() == union_op) return union_interpret(t[0], r);
	else if(t[0].get_type() == difference_op) return difference_interpret(t[0], r);
	else if(t[0].get_type() == product_op) return product_interpret(t[0], r);
	else return nullptr;
}

Table* DB_Set::atomic_expr_interpret(token& t, vector<Table*> r)
{
	if(t[0].get_type() == identifier) return (lookup(t[0].get_value()));
	else if(t[0].get_type() == expr) return (expr_interpret(t[0], r));
	else return nullptr;
}

Table* DB_Set::selection_interpret(token& t, vector<Table*> r)
{
	Condition cond = condition_interpret(t[0], r);
	Table* ptr = atomic_expr_interpret(t[1], r);
	if(ptr != nullptr)
		return Relation_Ops::select(cond, *ptr);
	else
		return nullptr;
}

Table* DB_Set::projection_interpret(token& t, vector<Table*> r)
{
	vector<string> attrs;
	token* itr = &(t[0]);
	while((*itr).get_type() == attribute_list)
	{
		attrs.push_back((*itr)[0].get_value());
		itr = &((*itr)[1]);
	}
	Table* ptr = atomic_expr_interpret(t[1], r);
	if(ptr != nullptr)
	{
		return Relation_Ops::project(attrs, *ptr);
	}
	else
		return nullptr;
}

Table* DB_Set::renaming_interpret(token& t, vector<Table*> r)
{
	vector<string> new_attrs, attrs;
	token* itr = &(t[0]);
	while((*itr).get_type() == attribute_list)
	{
		new_attrs.push_back((*itr)[0].get_value());
		itr = &((*itr)[1]);
	}
	Table* ptr = atomic_expr_interpret(t[1], r);
	vector<Attribute> attr_list = ptr->get_Template_Tuple().get_Attributes();
	for(int i = 0; i < attr_list.size(); ++i)
	{
		attrs.push_back(attr_list[i].get_Name());
	}
	if(ptr != nullptr)
	{
		Relation_Ops::rename(attrs, new_attrs, ptr);
		return ptr;
	}
	else
		return nullptr;
}

Table* DB_Set::union_interpret(token& t, vector<Table*> r)
{
	Table* ptr1 = atomic_expr_interpret(t[0], r);
	Table* ptr2 = atomic_expr_interpret(t[1], r);
	if (ptr1 != nullptr && ptr2 != nullptr)
		return Relation_Ops::set_union(*ptr1, *ptr2);
	else
	return nullptr;
}

Table* DB_Set::difference_interpret(token& t, vector<Table*> r)
{
	Table* ptr1 = atomic_expr_interpret(t[0], r);
	Table* ptr2 = atomic_expr_interpret(t[1], r);
	if (ptr1 != nullptr && ptr2 != nullptr)
		return Relation_Ops::set_diff(*ptr1, *ptr2);
	else
	return nullptr;
}

Table* DB_Set::product_interpret(token& t, vector<Table*> r)
{
	Table* ptr1 = atomic_expr_interpret(t[0], r);
	Table* ptr2 = atomic_expr_interpret(t[1], r);
	if (ptr1 != nullptr && ptr2 != nullptr)
		return Relation_Ops::cross_prod(*ptr1, *ptr2);
	else
	return nullptr;
}

Condition DB_Set::condition_interpret(token& t, vector<Table*> r)
{
	Condition left = conjunction_interpret(t[0], r);
	if(t[1].get_type() == empty)
		return left;
	else
	{
		Condition right = condition_interpret(t[1], r);
		Condition_Conj cond(left, right, "||");
		Condition c(cond);
		return c;
	}
}

Condition DB_Set::conjunction_interpret(token& t, vector<Table*> r)
{
	Condition left = comparison_interpret(t[0], r);
	if(t[1].get_type() == empty)
		return left;
	else
	{
		Condition right = conjunction_interpret(t[1], r);
		Condition_Conj cond(left, right, "&&");
		Condition c(cond);
		return c;
	}
}

Condition DB_Set::comparison_interpret(token& t, vector<Table*> r)
{
	if(t.get_value() == "OP")
	{
		string left, right;
		if(t[0][0].get_type() == identifier) left = t[0][0].get_value();
		else left = t[0][0][0].get_value();
		if(t[2][0].get_type() == identifier) right = t[2][0].get_value();
		else right = t[2][0][0].get_value();
		Condition_Sing op(left, t[1].get_value(), right);
		Condition c(op);
		return c;
	}
	else if(t.get_value() == "CONDITION")
		return condition_interpret(t[0], r);
    return condition_interpret(t[0],r);
}

void DB_Set::command_interpret(token& t, vector<Table*> r)
{
	if(t[0].get_type() == open_cmd) open_cmd_interpret(t[0], r);
	else if(t[0].get_type() == close_cmd) close_cmd_interpret(t[0], r);
	else if(t[0].get_type() == save_cmd) save_cmd_interpret(t[0], r);
	else if(t[0].get_type() == exit_cmd) exit_cmd_interpret(t[0], r);
	else if(t[0].get_type() == show_cmd) show_cmd_interpret(t[0], r);
	else if(t[0].get_type() == create_cmd) create_cmd_interpret(t[0], r);
	else if(t[0].get_type() == update_cmd) update_cmd_interpret(t[0], r);
	else if(t[0].get_type() == insert_cmd)insert_cmd_interpret(t[0], r);
	else if(t[0].get_type() == delete_cmd) delete_cmd_interpret(t[0], r);
}

void DB_Set::open_cmd_interpret(token& t, vector<Table*> r)
{
	string str;
	str.append(t[0].get_value());
	str.append(".db");
	Table* ptr = DB_Engine::open_relation(str);
	if(ptr != nullptr) tables.push_back(ptr);
	else cout<<"No table found\n";
}

void DB_Set::close_cmd_interpret(token& t, vector<Table*> r)
{
	string name = t[0].get_value();
	Table* ptr = lookup(name);
	name.append(".db");
	if(ptr != nullptr) 
	{
		int i = lookup(ptr);
		for(; i < tables.size()-1; ++i)
		{
			tables[i] = tables[i+1];
		}
		tables.resize(tables.size()-1);
		DB_Engine::close_relation(ptr, name);
	}
	else cout<<"No table found\n";
}

void DB_Set::save_cmd_interpret(token& t, vector<Table*> r)
{
	string name = t[0].get_value();
	Table* ptr = lookup(name);
	name.append(".db");
	if(ptr != nullptr) DB_Engine::save_relation(ptr, name);
	else cout<<"No table found\n";
}

void DB_Set::exit_cmd_interpret(token& t, vector<Table*> r)
{
	DB_Engine::exit_interp();
}

void DB_Set::show_cmd_interpret(token& t, vector<Table*> r)
{
	string name = t[0][0].get_value();
	Table* ptr = lookup(name);
	if(ptr != nullptr) DB_Engine::show(*ptr);
	else cout<<"No table found\n";
}

void DB_Set::create_cmd_interpret(token& t, vector<Table*> r)
{
	string name = t[0].get_value();
	vector<Attribute> attrs;
	token* itr = &(t[1]);
	while((*itr).get_type() == typed_attribute_list)
	{
		if((*itr)[1].get_value() == "INTEGER")
		{
			attrs.push_back(Attribute(0, false, (*itr)[0].get_value()));
		}
		else if((*itr)[1].get_value() == "VARCHAR")
		{
			attrs.push_back(Attribute("", false, (*itr)[0].get_value(), stoi((*itr)[1][0].get_value())));
		}
		itr = &((*itr)[2]);
	}
	itr = &(t[2]);
	while((*itr).get_type() == attribute_list)
	{
		for(int i = 0; i < attrs.size(); ++i)
		{
			if(attrs[i].get_Name() == (*itr)[0].get_value()) attrs[i].set_Is_Primary(true);
		}
		itr = &((*itr)[2]);
	}
	Tuple tmp();
	Table* tmp_ptr = DB_Engine::create_relation(name, Tuple(attrs));
	if(tmp_ptr != nullptr)
		tables.push_back(tmp_ptr);
}

void DB_Set::update_cmd_interpret(token& t, vector<Table*> r)
{
	string name = t[0].get_value();
	Table* ptr = lookup(name);
	token* itr = &(t[1]);
	vector<string> names, vals;
	while((*itr).get_type() == update_list)
	{
		names.push_back((*itr)[0].get_value());
		vals.push_back((*itr)[1][0].get_value());
		itr = &((*itr)[2]);
	}
	Condition cond = condition_interpret(t[2], r);
	if(ptr != nullptr) DB_Engine::update(ptr, names, vals, cond);
	else cout<<"No table found\n";
}

void DB_Set::insert_cmd_interpret(token& t, vector<Table*> r)
{
	string name = t[0].get_value();
	Table* ptr = lookup(name);
	if(ptr != nullptr)
	{
		if(t[1].get_type() == expr)
		{
			Table* e = expr_interpret(t[1], r);
			if(e != nullptr) DB_Engine::insert_relation(ptr, *e);
		}
		else if(t[1].get_type() == insert_list)
		{
			token* itr = &(t[1]);
			Tuple tup = ptr->get_Template_Tuple();
			vector<Attribute> attrs = tup.get_Attributes();
			int i = 0;
			while((*itr).get_type() == insert_list)
			{
				if(attrs[i].get_Length() == -1) attrs[i].set_Int_Value(stoi((*itr)[0][0].get_value()));
				else if(attrs[i].get_Length() != -1) attrs[i].set_String_Value((*itr)[0][0].get_value());
				itr = &((*itr)[1]);
				++i;
			}
			Tuple new_tup(attrs);
			DB_Engine::insert_tuple(ptr, attrs);
		}
	}
	else cout<<"No table found\n";
}

void DB_Set::delete_cmd_interpret(token& t, vector<Table*> r)
{
	string name = t[0].get_value();
	Table* ptr = lookup(name);
	Condition cond = condition_interpret(t[1], r);
	if(ptr != nullptr) DB_Engine::remove_query(ptr, cond);
	else cout<<"No table found\n";
}

/*DB_Set::~DB_Set()
{
	for(int i = 0; i < tables.size(); ++i)
	{
		if(tables[i] != nullptr) delete tables[i];
	}
}*/