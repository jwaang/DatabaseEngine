#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <algorithm>
#include "Table.h"
#include "Attribute.h"
#include "Condition.h"
using namespace std;

string strtolower(string s);

/*
	A token_type gives context and meaning to the token it is contained within
*/
enum token_type
{
	empty,
	program,
	fail,
	identifier,
	integer,
	literal_token,
	string_token,

	//query tokens:
	query,
	expr,
	atomic_expr,
	selection,
	condition, //2nd is either empty or another condition
	conjunction, //2nd is either empty or another conjunction
	comparison, //value is either OP or CONDITION
	op,
	operand,
	projection,
	attribute_list,
	renaming,
	union_op,
	difference_op,
	product_op,

	//command tokens:
	command,
	open_cmd,
	close_cmd,
	save_cmd,
	exit_cmd,
	show_cmd,
	create_cmd,
	update_cmd,
	update_list,
	insert_cmd,
	insert_list,
	delete_cmd,
	typed_attribute_list, //3rd is either empty or another conjunction
	type,
};

string token_type_to_str(token_type tt);
int token_type_sub_tokens(token_type tt);

/*
	Serves as a node on the token "tree"
	Knows its own type, and an hold a number f sub-tokens based on this type
	May or may not contain a string for additional information, depending on the type of the token. This string will have relevance when the token is evaluated
*/
class token
{
public:
	token() : type(empty), tokens(empty) {}
	token(token_type tt) : type(tt), tokens(vector<token>(token_type_sub_tokens(tt))), value("") {}
	token(token_type tt, string s) : type(tt), tokens(vector<token>(token_type_sub_tokens(tt))), value(s) {}
	token& operator[](int);
	token* pointer(int);
	string get_value() {return value;}
	token_type get_type() { return type; }
	void print(ostream& os);
	void print(ostream& os, int i);
private:
	token_type type;
	vector<token> tokens;
	string value;
};

/*
	From a user perspective, a parser merely takes a string and returns a token representing the structure of the string as a sort of tree
	The resulting token can be given to a yet to be created function in the DB system
	This function will be added during the integration phase of this assignment
*/
class Parser
{
public:
	Parser() {input.str("");}
	token parse(string);
private:
	stringstream input;	//contains the input currently being processed
	/*
		processes the contents of input into individual terms(strings). 
		The terms will be given structure in the parser.
		NOTE: Will consider the all of input invalid if an unrecognised character is found.
			  This means even if there are valid commands/queries within input, they will all be ignored if they are passed in with an invalid character
	*/
	vector<string> lex();	
	/*
		The below functions are all various sub-parsers.
		There are several low level parsers that directly interact with the results of lex(), these will be further explained.
		However, most of these parsers simply call lower level parsers in a particular order to define a higher level part of the grammar.
	*/
	token query_parse(vector<string>& lexed, int&);
	token identifier_parse(vector<string>& lexed, int&);
	token atomic_expr_parse(vector<string>& lexed, int&);
	token expr_parse(vector<string>& lexed, int&);
	token selection_parse(vector<string>& lexed, int&);
	token projection_parse(vector<string>& lexed, int&);
	token renaming_parse(vector<string>& lexed, int&);
	token union_parse(vector<string>& lexed, int&);
	token difference_parse(vector<string>& lexed, int&);
	token product_parse(vector<string>& lexed, int&);
	token command_parse(vector<string>& lexed, int&);
	token open_cmd_parse(vector<string>& lexed, int&);
	token close_cmd_parse(vector<string>& lexed, int&);
	token save_cmd_parse(vector<string>& lexed, int&);
	token exit_cmd_parse(vector<string>& lexed, int&);
	token show_cmd_parse(vector<string>& lexed, int&);
	token create_cmd_parse(vector<string>& lexed, int&);
	token update_cmd_parse(vector<string>& lexed, int&);
	token update_list_parse(vector<string>& lexed, int& i);
	token insert_cmd_parse(vector<string>& lexed, int&);
	token insert_list_parse(vector<string>& lexed, int& i);
	token delete_cmd_parse(vector<string>& lexed, int&);
	token condition_parse(vector<string>& lexed, int&);
	token conjunction_parse(vector<string>& lexed, int&);
	token comparison_parse(vector<string>& lexed, int&);
	token operand_parse(vector<string>& lexed, int&);
	token op_parse(vector<string>& lexed, int&);
	token literal_parse(vector<string>& lexed, int&);
	token typed_attribute_list_parse(vector<string>& lexed, int&);
	token attribute_list_parse(vector<string>& lexed, int&);
	token type_parse(vector<string>& lexed, int&);
	bool integer_check(vector<string>& lexed, int&); //verifies the value at position i is an integer by evaluating each character
	token integer_parse(vector<string>& lexed, int&); //after checking the value at position i is an integer, returns an integer/fail token based on this information
	bool word_parse(vector<string>& lexed, string*, int& i, int n); //Verifies the next n strings (starting from i) in lexed are the same as the strings from the character pointer
};