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
    std::ifstream file("../data/spreadspoke_scores.csv"); // import csv
    vector<string> games; // vec of games
    char delim = '\n'; // delimiter by newline
    int id = 0; // temporary way of tracking index of unique row
    std::string row; 

    while (std::getline(file, row, delim)) {
        games.push_back(row);
    }

    vector<unordered_map<string, string>> *ins_vec = new vector<unordered_map<string, string>>(); // vector to insert
    vector<string> *keys = new vector<string>(); // list keys

    for(auto it = games.begin() + 1; it != games.end(); it++) { 
        id += 1;
        // std::cout << *it << std::endl;
        string s = "";
        s += id;
        keys->push_back(s);
        unordered_map<string, string> *ins_row = new unordered_map<string, string>();
        string rd = "raw-data";


        (*ins_row)[rd] = *it;


        ins_vec->push_back(*ins_row);   

        // cout << ins_vec->id << endl;
    }
    DB->insert("games", ins_vec->size(), *keys, *ins_vec);
    DB->printDB();
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
