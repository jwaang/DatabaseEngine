#include "Relation_Ops.h"

/* Select tuples that satisfy condition from a given table */
Table* Relation_Ops::select(Condition cond, Table relation) {
	Table* new_table = new Table("select_table", relation.get_Template_Tuple()); // create new table pointer to store
	vector<Tuple> tuples = relation.get_Tuples(); // get the tuples
	vector<Tuple> new_tuples; // create new tuples to store in
	for (int i = 0; i < tuples.size(); i++) {
	    if(cond.eval_condition(tuples[i])) {
      		new_tuples.push_back(tuples[i]);
    	    }
	}

	for (int i = 0; i < new_tuples.size(); i++) {
		new_table->insert(new_tuples[i]); // insert the gotten tuples from condition and input into the new table
	}

	return new_table;
}
/* Select a subset of attributes from a given table */
Table* Relation_Ops::project(vector<string> attribute_names, Table relation) {
	
	Table* new_table = NULL; // New table

	
	if(relation.is_empty())
	{
		vector<Attribute> temp = relation.get_Template_Tuple().get_Attributes();
		vector<Attribute> new_attributes; // vector<Attribute> to be filled
		
		for (int j = 0; j < attribute_names.size(); j++) {
			for (int k = 0; k < temp.size(); k++) {
				if (temp[k].get_Name() == attribute_names[j]) { // check for equality between given relation and the given attribute names
					new_attributes.push_back(temp[k]); // if it is, push back into the new vector<Attribute>
				}
			}
		}
		Tuple new_tuple (new_attributes); // create a new tuple with the new vector
		new_table = new Table("project_table", new_tuple); // if first relation tuple, create the new temp for table 
	}
	
	for (int i = 0; i < relation.tuples.size(); i++) { 
		vector<Attribute> new_attributes; // vector<Attribute> to be filled
		vector<Attribute> temp = relation.tuples[i].get_Attributes(); // new filled vector<Attribute>
		for (int j = 0; j < attribute_names.size(); j++) {
			for (int k = 0; k < temp.size(); k++) {
				if (temp[k].get_Name() == attribute_names[j]) { // check for equality between given relation and the given attribute names
					new_attributes.push_back(temp[k]); // if it is, push back into the new vector<Attribute>
				}
			}
		}
		Tuple new_tuple (new_attributes); // create a new tuple with the new vector
		if (i == 0) {
			new_table = new Table("project_table", new_tuple); // if first relation tuple, create the new temp for table 
		}
		new_table->insert(new_tuple); // insert the new tuple
	}
	
	return new_table;
}
/* Rename selected attributes in a given table */
void Relation_Ops::rename(vector<string> attribute_names, vector<string> names, Table* relation) {
	vector<Attribute> temp;
	for (int k = 0; k < relation->tuples.size(); k++) {
		temp = relation->tuples[k].get_Attributes(); // get the attributes from the given relation and put in temp vector<Attribute>
		for (int i = 0; i < attribute_names.size(); i++) {
			for (int j = 0; j < temp.size(); j++) {
				if (attribute_names[i] == temp[j].get_Name()) { // check given relation to given names to change
					temp[j].set_Name(names[i]); // if good, change names with the given names to change to
				}
			}
		}
		relation->tuples[k] = temp;
	}
	relation->set_Template_Tuple(temp); // for the relation set the template tuple to temp

}
/* Given two tables, compute the the total combination of them */
Table* Relation_Ops::set_union(Table relation1, Table relation2) {
	bool check = relation1.is_Union_Compatible(relation2); // flag for union compatibility

	if (check) {
		Table* new_table = new Table(relation1 + relation2); // use overloaded operator to do set union
		return new_table;
	}
	else {
        Table* tmp = new Table(relation1);
        cerr << "Not union compatible" << endl;
        return tmp;
	}
}
/* Given two tables, compute the difference between them */
Table* Relation_Ops::set_diff(Table relation1, Table relation2) {
	bool check = relation1.is_Union_Compatible(relation2); // flag for union compatibility

	if (check) {
		Table* new_table = new Table(relation1 - relation2); // use overloaded operator to do set difference
		return new_table;
	}
	else {
        Table* tmp = new Table(relation1);
        cerr << "Not union compatible" << endl;
        return tmp;
	}
}
/* Given two tables, compute their Cartesian product */ 
Table* Relation_Ops::cross_prod(Table relation1, Table relation2) {
	Table* new_table = new Table(relation1 * relation2); // use overloaded operator to do cross product
	return new_table;
}
