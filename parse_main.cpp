/*#include "Parser.h"
#include <fstream>

int main()
{
	Parser p;
	vector<string> v;
	ofstream ofs;
	ofs.open("OUTPUT.txt");
	v.push_back("");
	v.push_back("opeN _th1z_1s_atest1 ;");
	v.push_back("ope _th1z_1s_atest1 ;");
	v.push_back("close close_cmd_test;");
	v.push_back("cloe close_cmd_test;");
	v.push_back("SAVE save_this;");
	v.push_back("sAVE 2save_this;");
	v.push_back("exit;");
	v.push_back("sHOW a1234;");
	v.push_back("sHOW (_testname);");
	v.push_back("sHOW (selecT (a > b) test);");
	v.push_back("sHOW (selecT a > b test);");
	v.push_back("sHOW (selecT (a > b) (Select ((_ != operand)) test));");
	v.push_back("sHOW (project (this, is, a , test) test22);");
	v.push_back("sHOW (project () test22);");
	v.push_back("sHOW (project (this,) test22);");
	v.push_back("sHOW (rename (this, is, a , test) test1);");
	v.push_back("sHOW (rename (this, is, a ,) test1);");
	v.push_back("sHOW ((rename (this, is, a , test) test1)+(project (this, too, is , test2) test22));");
	v.push_back("sHOW (+(project (this, too, is , test2) test22));");
	v.push_back("sHOW (name-(project (this, too, is , test2) test22));");
	v.push_back("sHOW (name-();");
	v.push_back("sHOW (name1*name2);");
	v.push_back("sHOW (*);");
	v.push_back("CREATE TABLE animals(test1 integer, name VARCHAR(20), kind VARCHAR(8), years INTEGER, test2 varchar(9001)) PRIMARY KEY(name, kind);");
	v.push_back("CREATE TABLE animals(test1 integer, name VARCHAR(20), kind VARCHAR(8), years INTEGER,) PRIMARY KEY(name, kind);");
	v.push_back("update pirates set booty = \"a_lot\", rum = 0 where abc == abc && b != b || c >= c;");
	v.push_back("update pirates set booty = \"a_lot\", where abc == abc && b != b || c >= c;");
	v.push_back("INSERT INTO species VALUES FROM RELATION project (kind) animals;");
	v.push_back("INSERT INTO species VALUES FROM RELATION project kind animals;");
	v.push_back("INSERT INTO animals VALUES FROM (\"Tweety\", \"bird\", 1, 44, \"_testing56\");");
	v.push_back("INSERT INTO animals VALUES FROM (Tweety, \"bird\", 1, 44, \"_testing56\");");
	v.push_back("deleTe FROM relation_name where 1 >= 2;");
	v.push_back("deleTe FROM 3relation_name where 1 >= 2;");
	v.push_back("fgyuewagbujc72szwP8C FW 798 2qgj7u;");
	v.push_back("quesry_test1<-select(1 == 2 || 3 == \"4\")test_over;");
	v.push_back("SAVE save_this;this is invalid;EXIT;");
	v.push_back("SAVE save_this;this is invalid EXIT;");
	v.push_back("failure;SAVE save_this;this is invalid; exit<- _a*_b;");
	v.push_back("exit; fail with no semicolon");
	v.push_back("show (select ((5 > 2)||((1 == 2 || \"a\" != \"b\") && (\"4\" != 4))) testing_condition);");
	v.push_back("show (select ((5 > 2)||((1 == 2 || \"a\" != \"b\") && \"4\" != 4)) testing_condition);");
	v.push_back("show (select (5 > 2||((1 == 2 || \"a\" != \"b\") && \"4\" != 4)) testing_condition);");
	v.push_back("show (select (5 > 2||1 == 2 || \"a\" != \"b\" && \"4\" != 4 || 4 < 3) testing_condition);");
	v.push_back("^%$");
	v.push_back("query1<-test;update treble set a = 1, b = 2, c = 3, d = \"d\" where 1==2||3==4&&5!=6||7>=8;fail;exit;query2<-computer+science");
	for (int i = 0; i < v.size(); ++i)
	{
		ofs << "Parsing: \"" << v[i] << "\"" << endl;
		token t = p.parse(v[i]);
		t.print(ofs);
		for(int i = 0; i < 500; ++i) ofs << "-";
		ofs << endl << endl;
	}
}*/