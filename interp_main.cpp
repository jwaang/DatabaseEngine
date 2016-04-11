#include "DB_Set.h"
#include "DB_Engine.h"
#include <string>
#include <iostream>
#include <fstream>

using namespace std;

//this program runs as an interpreter
//it parses user input as it is given
//it can also read an parse an input file
int main()
{
  
  string input;
  string filename;
  string line;
  DB_Set db_set;
  
  cout<<"\nWelcome to the Database interpreter. Please type a command.\n";
  cout<<"To open and parse a file, please type \"-o filename\"\n";
  cout<<"To quit please type the command \"EXIT;\"\n\n";
  
  DB_Engine::set_exit(false);
  
  while(!DB_Engine::get_exit())
  {
    cout<<">> ";
    getline(cin, input);
    
    //trim off whitespace at begginning
    while(input[0] == ' ')
    {
      input.erase(0,1);
    }
    
    //open a file
    if(input[0] == '-' && input[1] == 'o')
    {
      input.erase(0,3);
      filename = input;
      
      ifstream file(filename);

      if(!file.is_open())
      {
        cerr<<filename<<" failed to open. Please try a different file.\n";
      }
      else
      {
        while(getline(file, line))
        {
          if(line != "")
			db_set.input(line);
        }
      }
	  
	  DB_Engine::set_exit(false);
    }
    //parse input from command line
    else
    {
      db_set.input(input);
    }
  }
    
  cout<<"\nThank you for using the Database interpreter. Goodbye!\n";
}

    
  