#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <vector>
#include <sstream>
#include <map>
#include <unordered_map>
#include "SequentialDB.cpp"

using namespace std;

bool testBulkInsert(Seq_Database* DB) {
    std::ifstream file("../data/spreadspoke_scores.csv");
    std::string tmp; // temporary line (row)
    vector<string> games; // vec of games
    char delim = '\n'; // delimiter by newline


    while (std::getline(file, tmp, delim)) {
        games.push_back(tmp);
    }

    int id = 0;

    vector<unordered_map<string, string>> *ins_vec = new vector<unordered_map<string, string>>(); // outermost
    vector<int> keys = new vector<int>(); // list keys

    for(auto it = games.begin() + 1; it != games.end(); it++) { 
        id += 1;
        std::cout << *it << std::endl;
        // char column_field = strtok(*it, ","); // tokenize the game row
        // while(column_field != NULL) {
        unordered_map<string, string> *ins_row = new unordered_map<string, string>();
        keys->push_back(id);
            
        // }
        ins_row["raw-data"] = *it;
        ins_vec->push_back(ins_row);        
    }
    // DB->insert("games", ins_vec.size(), *keys, *ins_vec);
    return true;
}

int main() {

    Seq_Database* DB = new Seq_Database(); // testing sequential
   
    // -----------
    // read method goes here
    // -----------

    // -----------
    // insert method
    testBulkInsert(DB); // test bulk insert into db
    // -----------


    exit(EXIT_SUCCESS);
}
