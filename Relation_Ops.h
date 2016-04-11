#pragma once

#include "Table.h"
#include "Condition.h"
#include "Attribute.h"

#include <cstdlib>
#include <cctype>
#include <string>
#include <stdlib.h>

using namespace std;

class Relation_Ops {
public:
	// All functions are static so they can be called without creating an object
	static Table* select(Condition cond, Table relation); // Selection that gives back a Table pointer based on a given table and condition
	static Table* project(vector<string> attribute_names, Table relation); // Project gives back a Table pointer (subset of attributes) based on a given table and wanted attributes
	static void rename(vector<string> attribute_names, vector<string> names, Table* relation); // Rename the given relation pointer's attribute with the given strings
	static Table* set_union(Table relation1, Table relation2); // Do a set union on given tables if they're union compatible
	static Table* set_diff(Table relation1, Table relation2); // Do a set difference on given tables if they're union compatible
	static Table* cross_prod(Table relation1, Table relation2); // Do a cross product on given tables
};