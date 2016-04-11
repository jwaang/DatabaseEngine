#include "Parser.h"

string strtolower(string s)
{
	char c;
	for (int i = 0; i < s.size(); ++i)
	{
		c = s[i];
		c = tolower(c);
		s[i] = c;
	}
	return s;
}

string token_type_to_str(token_type tt)
{
	switch (tt)
	{
	case (empty) : return "empty"; break;
	case (program) : return "program"; break;
	case (fail) : return "fail"; break;
	case (identifier) : return "identifier"; break;
	case (integer) : return "integer"; break;
	case (literal_token) : return "literal_token"; break;
	case (string_token) : return "string_token"; break;
	case (query) : return "query"; break;
	case (expr) : return "expr"; break;
	case (atomic_expr) : return "atomic_expr"; break;
	case (selection) : return "selection"; break;
	case (condition) : return "condition"; break;
	case (conjunction) : return "conjunction"; break;
	case (comparison) : return "comparison"; break;
	case (op) : return "op"; break;
	case (operand) : return "operand"; break;
	case (projection) : return "projection"; break;
	case (attribute_list) : return "attribute_list"; break;
	case (renaming) : return "renaming"; break;
	case (union_op) : return "union_op"; break;
	case (difference_op) : return "difference_op"; break;
	case (product_op) : return "product_op"; break;
	case (command) : return "command"; break;
	case (open_cmd) : return "open_cmd"; break;
	case (close_cmd) : return "close_cmd"; break;
	case (save_cmd) : return "save_cmd"; break;
	case (exit_cmd) : return "exit_cmd"; break;
	case (show_cmd) : return "show_cmd"; break;
	case (create_cmd) : return "create_cmd"; break;
	case (update_cmd) : return "update_cmd"; break;
	case (update_list) : return "update_list"; break;
	case (insert_cmd) : return "insert_cmd"; break;
	case (insert_list) : return "insert_list"; break;
	case (delete_cmd) : return "delete_cmd"; break;
	case (typed_attribute_list) : return "typed_attribute_list"; break;
	case (type) : return "type"; break;
	default: return "?";
	}
}

//Converts a token type to an int which represents the number of sub-tokens it will possess
int token_type_sub_tokens(token_type tt)
{
	switch (tt)
	{
		//general tokens:
		case (empty) : return 0; break;
		case (program) : return 2; break;
		case (fail) : return 0; break;
		case (identifier) : return 0; break;
		case (integer) : return 0; break;
		case (literal_token) : return 1; break;
		case (string_token) : return 0; break;

		//query tokens:
		case (query) : return 2; break;
		case (expr) : return 1; break;
		case (atomic_expr) : return 1; break;
		case (selection) : return 2; break;
		case (condition) : return 2; break; //2nd is either empty or another condition
		case (conjunction) : return 2; break; //2nd is either empty or another conjunction
		case (comparison) : return 3; break; //value is either OP or CONDITION
		case (op) : return 0; break;
		case (operand) : return 1; break;
		case (projection) : return 2; break;
		case (attribute_list) : return 2; break; //own value followed by empty/another attribute_list
		case (renaming) : return 2; break;
		case (union_op) : return 2; break;
		case (difference_op) : return 2; break;
		case (product_op) : return 2; break;

		//command tokens:
		case (command) : return 1; break;
		case (open_cmd) : return 1; break;
		case (close_cmd) : return 1; break;
		case (save_cmd) : return 1; break;
		case (exit_cmd) : return 0; break;
		case (show_cmd) : return 1; break;
		case (create_cmd) : return 3; break;
		case (update_cmd) : return 3; break;
		case (update_list) : return 3; break;
		case (insert_cmd) : return 2; break;
		case (insert_list) : return 2; break;
		case (delete_cmd) : return 2; break;
		case (typed_attribute_list) : return 3; break; //3rd is either empty or another conjunction
		case (type) : return 1; break;
		default: return -1;
	}
}

void token::print(ostream& os)
{
	int i = 0;
	os << token_type_to_str(type) << "(" << value << ")" << endl;
	for (int j = 0; j < token_type_sub_tokens(type); ++j) tokens[j].print(os, i + 1);
}

void token::print(ostream& os, int i)
{
	for (int j = 0; j < i; ++j) os << "\t";
	os << token_type_to_str(type) << "(" << value << ")" << endl;
	for (int j = 0; j < token_type_sub_tokens(type); ++j) tokens[j].print(os, i + 1);
}

/*
	Simply takes a string and returns a token representing the determined value of the string
	This token will later be consumed by a function in the database manager
	In this sense, the token is a sort of instruction set for what the DBM should do when called
	To see the contents of the token print may be called
	The output can be interpreted as follows:
		-Most tokens contain sub tokens, which are denoted by lower tokens tabbed once over
		-If a token "chains" in a list, such as list of commands/queries, attributes, or typed-attributes, this will be denoted by having the final subtoken be another x token in the chain of x's
		-The end of a list will be denoted by an empty token instead of an x
		-A program refers to a query/command, and chain as aforementioned
		-If a query/command is invalid, the program will contain a fail token instead
		An example:
			program()
				command()
					save_cmd()
						identifier(save_this)
				program()
					fail()
					program()
						command()
							exit_cmd()
						empty()
		This token represented 3 commands
			1) A save command with an identifier
			2) An invalid command
			3) An exit command
		Some larger tokens can be difficult to read, however for these purposes it should be noted that if a command is considered
		valid its program token will contain something other than a fail token. What that valid token represents is irrelevant (in this stage), the fact
		it is not a fail token means the string contained valid input.

*/
token Parser::parse(string i)
{
	int pos = 0;
	token head(program);
	head[0] = token(fail);
	head[1] = token(empty);
	token tmp;
	token* current = &head;
	input.str(i);
	vector<string> lexed;
	lexed = lex();
	while (pos < lexed.size())
	{
		tmp = token(program);
		tmp[1] = token(empty);
		tmp[0] = query_parse(lexed, pos);
		if(tmp[0].get_type() != fail)
		{
			*current = tmp;
			current = current->pointer(1);
			continue;
		}
		tmp[0] = command_parse(lexed, pos);
		if(tmp[0].get_type() != fail)
		{
			*current = tmp;
			current = current->pointer(1);
			continue;
		}
		*current = tmp;
		current = current->pointer(1);
		for(; pos < lexed.size(); ++pos)
		{
			if(lexed[pos] == ";")
			{
				++pos;
				break;
			}
		}
	}
	input.clear();
	return head;
}

token& token::operator[](int i)
{
	return tokens[i];
}

token* token::pointer(int i) { return tokens.data()+i;}

//Cycles through input one char at a time, determines if has meaning/gives meaning to following chars, and pushes it/them into a vector of strings
//Will consider the entire input invalid if an unrecognized char is encountered
vector<string> Parser::lex()
{
	char tmp;
	stringstream word;
	vector<string> words;
	while (input.good())
	{
		input.get(tmp);
		if (input.eof()) break;
		if (tmp == '+' ||
			tmp == '-' ||
			tmp == '*' ||
			tmp == '(' ||
			tmp == ')' ||
			tmp == ',' ||
			tmp == ';')
		{
			word.put(tmp);
			words.push_back(word.str());
		}
		else if (isdigit(tmp))
		{
			word.put(tmp);
			while (!(input.eof()) && isdigit(input.peek()))
			{
				input.get(tmp);
				word.put(tmp);
			}
			words.push_back(word.str());
		}
		else if (tmp == '<')
		{
			if (char(input.peek()) == '=')
			{
				word.put(tmp);
				input.get(tmp);
				word.put(tmp);
				words.push_back(word.str());
			}
			else if (char(input.peek()) == '-')
			{
				word.put(tmp);
				input.get(tmp);
				word.put(tmp);
				words.push_back(word.str());
			}
			else
			{
				word.put(tmp);
				words.push_back(word.str());
			}
		}
		else if (tmp == '>')
		{
			if (char(input.peek()) == '=')
			{
				word.put(tmp);
				input.get(tmp);
				word.put(tmp);
				words.push_back(word.str());
			}
			else
			{
				word.put(tmp);
				words.push_back(word.str());
			}
		}
		else if (tmp == '!' && char(input.peek()) == '=')
		{
			word.put(tmp);
			input.get(tmp);
			word.put(tmp);
			words.push_back(word.str());
		}
		else if (tmp == '=')
		{
			if (char(input.peek()) == '=')
			{
				word.put(tmp);
				input.get(tmp);
				word.put(tmp);
				words.push_back(word.str());
			}
			else
			{
				word.put(tmp);
				words.push_back(word.str());
			}
		}
		else if (tmp == '&' && char(input.peek()) == '&')
		{
			word.put(tmp);
			input.get(tmp);
			word.put(tmp);
			words.push_back(word.str());
		}
		else if (tmp == '|' && char(input.peek()) == '|')
		{
			word.put(tmp);
			input.get(tmp);
			word.put(tmp);
			words.push_back(word.str());
		}
		else if (isalpha(tmp) || tmp == '_')
		{
			word.put(tmp);
			while (!(input.eof()) && (isdigit(input.peek()) || isalpha(input.peek()) || input.peek() == '_'))
			{
				input.get(tmp);
				word.put(tmp);
			}
			words.push_back(word.str());
		}
		else if(tmp == '\"')
		{
			word.put(tmp);
			words.push_back(word.str());
			word.str("");
			while (!(input.eof()) && (input.peek() != '\"'))
			{
				input.get(tmp);
				word.put(tmp);
			}
			words.push_back(word.str());
			word.str("");
			input.get(tmp);
			if(tmp == '\"')
			{
				word.put(tmp);
				words.push_back(word.str());
				word.str("");
			}
			else 
			{
				words = vector<string>(0);
				return words;
			}
		}
		else if (tmp == ' ' || tmp == '\n'){}
		else
		{
			words = vector<string>(0);
			break;
		}
		word.str("");
	}
	return words;
}

//All functions below this pointer are various parsers.
//Their implementation is directly reflected by the grammar
//They are mostly centered around calling each other in a fashion following the grammar's format
token Parser::query_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token id = identifier_parse(lexed, i);
	if(id.get_type() != fail)
	{
		string str1[] = {"<-"}; 
		bool tmp = word_parse(lexed, str1, i, 1);
		if(tmp)
		{
			token expr = expr_parse(lexed, i);
			if(expr.get_type() != fail)
			{
				string str2[] = { ";" };
				bool tmp = word_parse(lexed, str2, i, 1);
				{
					token q(query);
					q[0] = id;
					q[1] = expr;
					return q;
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::identifier_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	if (lexed.size() >= i + 1)
	{
		string& tmp = lexed[i];
		if (isalpha(tmp[0]) || tmp[0] == '_')
		{
			bool id(true);
			for (int j = 0; j < tmp.size(); ++j)
			{
				if (!(isalpha(tmp[j]) || isdigit(tmp[j]) || tmp[j] == '_'))
				{
					id == false;
					break;
				}
			}
			if (id)
			{
				++i;
				return token(identifier, lexed[i-1]);
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::atomic_expr_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token id = identifier_parse(lexed, i);
	if(id.get_type() != fail)
	{
		token ae1(atomic_expr);
		ae1[0] = id;
		return ae1;
	}
	string str1[] = { "(" };
	if (word_parse(lexed, str1, i, 1))
	{
		token expr = expr_parse(lexed, i);
		if(expr.get_type() != fail)
		{
			string str2[] = { ")" };
			if (word_parse(lexed, str2, i, 1))
			{
				token ae2(atomic_expr);
				ae2[0] = expr;
				return ae2;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::expr_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token exp(expr);
	token e;
	e = selection_parse(lexed, i);
	if (e.get_type() != fail)
	{
		exp[0] = e;
		return exp;
	}
	e = projection_parse(lexed, i);
	if (e.get_type() != fail)
	{
		exp[0] = e;
		return exp;
	}
	e = renaming_parse(lexed, i);
	if (e.get_type() != fail)
	{
		exp[0] = e;
		return exp;
	}
	e = union_parse(lexed, i);
	if (e.get_type() != fail)
	{
		exp[0] = e;
		return exp;
	}
	e = difference_parse(lexed, i);
	if (e.get_type() != fail)
	{
		exp[0] = e;
		return exp;
	}
	e = product_parse(lexed, i);
	if (e.get_type() != fail)
	{
		exp[0] = e;
		return exp;
	}
	e = atomic_expr_parse(lexed, i);
	if (e.get_type() != fail)
	{
		exp[0] = e;
		return exp;
	}
	i = original_pos;
	return token(fail);
}

token Parser::selection_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "select", "(" };
	if (word_parse(lexed, str1, i, 2))
	{
		token cond = condition_parse(lexed, i);
		if (cond.get_type() != fail)
		{
			string str2[] = { ")" };
			if (word_parse(lexed, str2, i, 1))
			{
				token ae = atomic_expr_parse(lexed, i);
				if(ae.get_type() != fail)
				{
					token s(selection);
					s[0] = cond;
					s[1] = ae;
					return s;
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::projection_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = {"project", "("};
	if(word_parse(lexed, str1, i, 2))
	{
		token al = attribute_list_parse(lexed, i);
		if(al.get_type() != fail)
		{
			string str2[] = {")"};
			if(word_parse(lexed, str2, i, 1))
			{
				token ae = atomic_expr_parse(lexed, i);
				if(ae.get_type() != fail)
				{
					token proj(projection);
					proj[0] = al;
					proj[1] = ae;
					return proj;
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::renaming_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "rename", "(" };
	if (word_parse(lexed, str1, i, 2))
	{
		token al = attribute_list_parse(lexed, i);
		if (al.get_type() != fail)
		{
			string str2[] = { ")" };
			if (word_parse(lexed, str2, i, 1))
			{
				token ae = atomic_expr_parse(lexed, i);
				if (ae.get_type() != fail)
				{
					token rn(renaming);
					rn[0] = al;
					rn[1] = ae;
					return rn;
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::union_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token ae1 = atomic_expr_parse(lexed, i);
	if (ae1.get_type() != fail)
	{
		string str1[] = { "+" };
		if (word_parse(lexed, str1, i, 1))
		{
			token ae2 = atomic_expr_parse(lexed, i);
			if (ae2.get_type() != fail)
			{
				token u(union_op);
				u[0] = ae1;
				u[1] = ae2;
				return u;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::difference_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token ae1 = atomic_expr_parse(lexed, i);
	if (ae1.get_type() != fail)
	{
		string str1[] = { "-" };
		if (word_parse(lexed, str1, i, 1))
		{
			token ae2 = atomic_expr_parse(lexed, i);
			if (ae2.get_type() != fail)
			{
				token d(difference_op);
				d[0] = ae1;
				d[1] = ae2;
				return d;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::product_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token ae1 = atomic_expr_parse(lexed, i);
	if (ae1.get_type() != fail)
	{
		string str1[] = { "*" };
		if (word_parse(lexed, str1, i, 1))
		{
			token ae2 = atomic_expr_parse(lexed, i);
			if (ae2.get_type() != fail)
			{
				token p(product_op);
				p[0] = ae1;
				p[1] = ae2;
				return p;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::command_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token comm(command);
	token c = open_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	c = close_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	c = save_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	c = exit_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	c = show_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	c = create_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	c = update_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	c = insert_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	c = delete_cmd_parse(lexed, i);
	if (c.get_type() != fail)
	{
		comm[0] = c;
		return comm;
	}
	i = original_pos;
	return token(fail);
}

token Parser::open_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "open" };
	if (word_parse(lexed, str1, i, 1))
	{
		token id = identifier_parse(lexed, i);
		if (id.get_type() != fail)
		{
			string str2[] = { ";" };
			if (word_parse(lexed, str2, i, 1))
			{
				token t(open_cmd);
				t[0] = id;
				return t;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::close_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "close" };
	if (word_parse(lexed, str1, i, 1))
	{
		token id = identifier_parse(lexed, i);
		if (id.get_type() != fail)
		{
			string str2[] = { ";" };
			if (word_parse(lexed, str2, i, 1))
			{
				token t(close_cmd);
				t[0] = id;
				return t;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::save_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "save" };
	if (word_parse(lexed, str1, i, 1))
	{
		token id = identifier_parse(lexed, i);
		if (id.get_type() != fail)
		{
			string str2[] = { ";" };
			if (word_parse(lexed, str2, i, 1))
			{
				token t(save_cmd);
				t[0] = id;
				return t;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::exit_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "exit" };
	if (word_parse(lexed, str1, i, 1))
	{
		string str2[] = { ";" };
		if (word_parse(lexed, str2, i, 1))
		{
			token t(exit_cmd);
			return t;
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::show_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "show" };
	if (word_parse(lexed, str1, i, 1))
	{
		token id = atomic_expr_parse(lexed, i);
		if (id.get_type() != fail)
		{
			string str2[] = { ";" };
			if (word_parse(lexed, str2, i, 1))
			{
				token t(show_cmd);
				t[0] = id;
				return t;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::create_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "CREATE", "TABLE" };
	if (word_parse(lexed, str1, i, 2))
	{
		token id = identifier_parse(lexed, i);
		if (id.get_type() != fail)
		{
			string str2[] = { "(" };
			if (word_parse(lexed, str2, i, 1))
			{
				token tal = typed_attribute_list_parse(lexed, i);
				if (tal.get_type() != fail)
				{
					string str3[] = { ")", "PRIMARY", "KEY", "(" };
					if (word_parse(lexed, str3, i, 4))
					{
						token al = attribute_list_parse(lexed, i);
						if (al.get_type() != fail)
						{
							string str4[] = { ")" };
							if (word_parse(lexed, str4, i, 1))
							{
								string str2[] = { ";" };
								if (word_parse(lexed, str2, i, 1))
								{
									token t(create_cmd);
									t[0] = id;
									t[1] = tal;
									t[2] = al;
									return t;
								}
							}
						}
					}
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::update_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = {"update"};
	if (word_parse(lexed, str1, i, 1))
	{
		token id1 = identifier_parse(lexed, i);
		if (id1.get_type() != fail)
		{
			string str2[] = { "set" };
			if (word_parse(lexed, str2, i, 1))
			{
				token ul = update_list_parse(lexed, i);
				if (ul.get_type() != fail)
				{
					string str3[] = { "where" };
					if (word_parse(lexed, str3, i, 1))
					{
						token cond = condition_parse(lexed, i);
						if (cond.get_type() != fail)
						{
							string str4[] = { ";" };
							if (word_parse(lexed, str4, i, 1))
							{
								token ud(update_cmd);
								ud[0] = id1;
								ud[1] = ul;
								ud[2] = cond;
								return ud;
							}
						}
					}
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::update_list_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token id1 = identifier_parse(lexed, i);
	if (id1.get_type() != fail)
	{
		string str1[] = { "=" };
		if (word_parse(lexed, str1, i, 1))
		{
			token lit = literal_parse(lexed, i);
			if (lit.get_type() != fail)
			{
				string str2[] = { "," };
				if (word_parse(lexed, str2, i, 1))
				{
					token recur = update_list_parse(lexed, i);
					if (recur.get_type() != fail)
					{
						{
							token ul(update_list);
							ul[0] = id1;
							ul[1] = lit;
							ul[2] = recur;
							return ul;
						}
					}
				}
				else
				{
					token ul(update_list);
					ul[0] = id1;
					ul[1] = lit;
					ul[2] = token(empty);
					return ul;
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::insert_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "insert", "into" };
	if (word_parse(lexed, str1, i, 2))
	{
		token id1 = identifier_parse(lexed, i);
		if (id1.get_type() != fail)
		{
			string str2[] = { "values", "from" };
			if (word_parse(lexed, str2, i, 2))
			{
				string str3[] = { "relation"};
				if (word_parse(lexed, str3, i, 1))
				{
					token expr = expr_parse(lexed, i);
					if (expr.get_type() != fail)
					{
						string str4[] = { ";" };
						if (word_parse(lexed, str4, i, 1))
						{
							token insert(insert_cmd);
							insert[0] = id1;
							insert[1] = expr;
							return insert;
						}
					}
				}
				else
				{
					string str4[] = { "(" };
					if (word_parse(lexed, str4, i, 1))
					{
						token il = insert_list_parse(lexed, i);
						if (il.get_type() != fail)
						{
							string str5[] = { ")", ";" };
							if (word_parse(lexed, str5, i, 2))
							{
								token in(insert_cmd);
								in[0] = id1;
								in[1] = il;
								return in;
							}
						}
					}
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::insert_list_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token lit = literal_parse(lexed, i);
	if (lit.get_type() != fail)
	{
		string str1[] = { "," };
		if (word_parse(lexed, str1, i, 1))
		{
			token recur = insert_list_parse(lexed, i);
			if (recur.get_type() != fail)
			{
				token il(insert_list);
				il[0] = lit;
				il[1] = recur;
				return il;
			}
		}
		else
		{
			token il(insert_list);
			il[0] = lit;
			il[1] = token(empty);
			return il;
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::delete_cmd_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "delete", "from" };
	if (word_parse(lexed, str1, i, 2))
	{
		token id = identifier_parse(lexed, i);
		if (id.get_type() != fail)
		{
			string str2[] = { "where" };
			if (word_parse(lexed, str2, i, 1))
			{
				token cond = condition_parse(lexed, i);
				if (cond.get_type() != fail)
				{
					string str3[] = { ";" };
					if (word_parse(lexed, str3, i, 1))
					{
						token t(delete_cmd);
						t[0] = id;
						t[1] = cond;
						return t;
					}
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::condition_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token conj1 = conjunction_parse(lexed, i);
	if (conj1.get_type() != fail)
	{
		string str1[] = { "||" };
		if (word_parse(lexed, str1, i, 1))
		{
			token recur = condition_parse(lexed, i);
			if (recur.get_type() != fail)
			{
				token cond(condition);
				cond[0] = conj1;
				cond[1] = recur;
				return cond;
			}
		}
		else
		{
			token cond(condition);
			cond[0] = conj1;
			cond[1] = token(empty);
			return cond;
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::conjunction_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token comp = comparison_parse(lexed, i);
	if (comp.get_type() != fail)
	{
		string str1[] = { "&&" };
		if (word_parse(lexed, str1, i, 1))
		{
			token recur = conjunction_parse(lexed, i);
			if (recur.get_type() != fail)
			{
				token conj(conjunction);
				conj[0] = comp;
				conj[1] = recur;
				return conj;
			}
		}
		else
		{
			token conj(conjunction);
			conj[0] = comp;
			conj[1] = token(empty);
			return conj;
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::comparison_parse(vector<string>& lexed, int&i)
{
	int original_pos = i;
	token oprnd1 = operand_parse(lexed, i);
	if (oprnd1.get_type() != fail)
	{
		token op = op_parse(lexed, i);
		if (op.get_type() != fail)
		{
			token oprnd2 = operand_parse(lexed, i);
			if (oprnd2.get_type() != fail)
			{
				token comp(comparison, "OP");
				comp[0] = oprnd1;
				comp[1] = op;
				comp[2] = oprnd2;
				return comp;
			}
		}
	}
	string str1[] = {"("};
	if(word_parse(lexed, str1, i, 1))
	{
		token cond = condition_parse(lexed, i);
		if(cond.get_type() != fail)
		{
			string str2[] = {")"};
			if(word_parse(lexed, str2, i, 1))
			{
				token comp2(comparison, "CONDITION");
				comp2[0] = cond;
				return comp2;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::operand_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token id = identifier_parse(lexed, i);
	if (id.get_type() != fail)
	{
		token oprnd(operand);
		oprnd[0] = id;
		return oprnd;
	}
	token lit = literal_parse(lexed, i);
	if (lit.get_type() != fail)
	{
		token oprnd(operand);
		oprnd[0] = lit;
		return oprnd;
	}
	i = original_pos;
	return token(fail);
}

token Parser::op_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	if(lexed.size() >= i+1)
	{
		if(lexed[i] == "==") {++i; return token(op, "==");}
		else if (lexed[i] == "!=") { ++i; return token(op, "!="); }
		else if (lexed[i] == "<") { ++i; return token(op, "<"); }
		else if (lexed[i] == ">") { ++i; return token(op, ">"); }
		else if (lexed[i] == "<=") { ++i; return token(op, "<="); }
		else if (lexed[i] == ">=") { ++i; return token(op, ">="); }
	}
	i = original_pos;
	return token(fail);
}

token Parser::literal_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token integ = integer_parse(lexed, i);
	if(integ.get_type() != fail)
	{
		token lit(literal_token);
		lit[0] = integ;
		return lit;
	}
	string str1[] = {"\""};
	if(word_parse(lexed, str1, i, 1))
	{
		if(lexed.size() > i+1)
		{
			token id = token(string_token, lexed[i]);
			++i;
			string str2[] = {"\""};
			if(word_parse(lexed, str2, i, 1))
			{
				token lit(literal_token);
				lit[0] = id;
				return lit;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::typed_attribute_list_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token id1 = identifier_parse(lexed, i);
	if (id1.get_type() != fail)
	{
		token t = type_parse(lexed, i);
		if(t.get_type() != fail)
		{
			string str1[] = { "," };
			if (word_parse(lexed, str1, i, 1))
			{
				token recur = typed_attribute_list_parse(lexed, i);
				if (recur.get_type() != fail)
				{
					token tal(typed_attribute_list);
					tal[0] = id1;
					tal[1] = t;
					tal[2] = recur;
					return tal;
				}
			}
			else
			{
				token tal(typed_attribute_list);
				tal[0] = id1;
				tal[1] = t;
				tal[2] = token(empty);
				return tal;
			}
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::attribute_list_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	token id1 = identifier_parse(lexed, i);
	if (id1.get_type() != fail)
	{
		string str1[] = { "," };
		if (word_parse(lexed, str1, i, 1))
		{
			token recur = attribute_list_parse(lexed, i);
			if (recur.get_type() != fail)
			{
				token al(attribute_list);
				al[0] = id1;
				al[1] = recur;
				return al;
			}
		}
		else
		{
			token al(attribute_list);
			al[0] = id1;
			al[1] = token(empty);
			return al;
		}
	}
	i = original_pos;
	return token(fail);
}

token Parser::type_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	string str1[] = { "integer" };
	if (word_parse(lexed, str1, i, 1))
	{
		token t1(type, "INTEGER");
		t1[0] = token(empty);
		return t1;
	}
	else if(lexed.size() >= i + 2)
	{
		string str2[] = { "VARCHAR", "(" };
		if (word_parse(lexed, str2, i, 2))
		{
			token integ = integer_parse(lexed, i);
			if(integ.get_type() != fail)
			{
				string str3[] = { ")" };
				if(word_parse(lexed, str3, i, 1))
				{
					token t2(type, "VARCHAR");
					t2[0] = integ;
					return t2;
				}
			}
		}
	}
	i = original_pos;
	return token(fail);
}

bool Parser::integer_check(vector<string>& lexed, int& i)
{
	if (lexed.size() >= i + 1)
	{
		bool valid = true;
		for (int j = 0; j < lexed[i].size(); j++)
		{
			if (!(isdigit(lexed[i][j]))) valid = false;
		}
		return valid;
	}
	return false;
}

token Parser::integer_parse(vector<string>& lexed, int& i)
{
	int original_pos = i;
	if (integer_check(lexed, i))
	{
		++i;
		return token(integer, lexed[i-1]);
	}
	i = original_pos;
	return token(fail);
}

bool Parser::word_parse(vector<string>& lexed, string* s, int& i, int n)
{
	int original_pos = i;
	if(lexed.size() >= i+n)
	{
		for(int j = 0; j < n; ++j)
		{
			if (strtolower(s[j]) != strtolower(lexed[i + j])) { return false; }
		}
		i += n;
		return true;
	}
	i = original_pos;
	return false;
}