#include "Parser.h"
#include <iostream>
using namespace std;

int main()
{
	Parser p;
	string s;
	while(true)
	{
		getline(cin, s);
		cout << "------------------------\n";
		token t = p.parse(s);
		t.print(cout);
	}
}