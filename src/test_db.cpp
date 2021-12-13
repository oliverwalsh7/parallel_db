#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <vector>
#include <sstream>
#include <map>
#include <unordered_map>
#include <chrono>
#include "SequentialDB.cpp"

using namespace std;

vector<unordered_map<string,string>> segmentData() {
   
    std::ifstream file("../data/nfl-0.csv"); // import csv
    vector<string> games; // vec of games
    string s = ""; // for tracking label rows
    char delim = '\n'; // delimiter by newline
    char row_delim = ','; // delimiter by newline
    unordered_map<string, string> *final_row = new unordered_map<string, string>(); // final map to insert   
    
    std::string row; 
    vector<string> *keys = new vector<string>(); // list keys
    unordered_map<string, string> *ins_row = new unordered_map<string, string>();
    vector<unordered_map<string, string>> *ins_vec = new vector<unordered_map<string, string>>(); // vector to insert

    while (std::getline(file, row, delim)) {
        games.push_back(row); // vector of rows, raw
    }

    string labels = games[0]; // labels row
    cout << labels << endl;

    stringstream s_stream(labels);

    while (getline(s_stream, s, row_delim)) {
        keys->push_back(s); // push back keys
    }
    
    for(auto it = games.begin() + 1; it != games.end(); it++) { 
        unordered_map<string,string> *delimited_row = new unordered_map<string,string>();
        int c = 0; // horizontal count commas
        stringstream s_stream(*it);

        while (getline(s_stream, s, row_delim)) {
            string x = keys->at(c);
            delimited_row->insert(make_pair<string,string>(move(x),move(s))); // push back keys
            c++;
        }
        ins_vec->push_back(*delimited_row);
    }

    return *ins_vec;
}

bool testBulkInsert(Seq_Database* DB) {
    vector<string> *keys = new vector<string>(); // list keys
    vector<unordered_map<string,string>> ins_vec = segmentData();
    DB->insert("games", ins_vec.size(), *keys, ins_vec);
    // DB->printDB();
    return true;
}


// bool testGet(Seq_Database* DB, pair<string,string> c1) { // c1 = criteria
//     vector<pair<string, string>>* conds = new vector<pair<string, string>>();
//     vector<pair<string, string>>* conds2 = new vector<pair<string, string>>();
//     conds->push_back(c1);
//     vector<unordered_map<string, string>> results = DB->get("games", *conds);
//     for (unordered_map<string,string> x : results){
//         for(auto y : x){
//             cout << y.first << ": " << y.second << ", ";
//         }
//         cout << endl << endl;
//     }

//     results = DB->get("games", *conds);
//     for (unordered_map<string,string> x : results){
//         for(auto y : x){
//             cout <<"Post- "<< y.first << ": " << y.second << ", ";
//         }
//         cout << endl << endl;
//     }
//     return true;
// }

bool testGet(Seq_Database* DB) { // c1, c2 = criteria
    tuple<string, string, int> home = make_tuple<string, string>("team_home", "New England Patriots", 0);
    tuple<string, string, int> away = make_tuple<string, string>("team_away", "Buffalo Bills", 0);
    tuple<string, string, int> score = make_tuple<string, string>("score_home", "14",1);
    tuple<string, string, int> score2 = make_tuple<string, string>("score_home", "17", -1);
    vector<tuple<string, string, int>>* conds = new vector<tuple<string, string, int>>();
    vector<tuple<string, string, int>>* conds2 = new vector<tuple<string, string, int>>();
    vector<tuple<string, string, int>>* conds3 = new vector<tuple<string, string, int>>();

    conds->push_back(home);
    conds2->push_back(away);
    conds3->push_back(score);
    conds3->push_back(score2);
    vector<unordered_map<string, string>> results = DB->get("games", *conds3);
    for (unordered_map<string,string> x : results){
        for(auto y : x){
            cout << y.first << ": " << y.second << ", ";
        }
        cout << endl << endl;
    }
    cout << "Total records: " << results.size() << endl;
    return true;
}

bool testRemove(Seq_Database *DB, pair<string,string> c1) { // singular remove
    vector<pair<string, string>>* conds = new vector<pair<string, string>>();
    conds->push_back(c1);
    DB->remove("games", *conds);
    return true;
}

int main() {
    Seq_Database* DB = new Seq_Database(); // testing sequential

    // ----- insert ------
    testBulkInsert(DB);
    // -------------------

    // ----- get ---------
    testGet(DB);
    // -------------------

    // ----- remove ------
    // pair<string, string> rm = make_pair<string, string>("score_home", "10");
    // testRemove(DB, rm);
    // cout << "POST:" << endl;
    // testGet(DB);
    // -------------------



    exit(EXIT_SUCCESS);
}
