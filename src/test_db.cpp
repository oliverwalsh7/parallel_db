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
#include <valarray>
#include "SequentialDB.cpp"
#include <random>

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

bool populateDB(Seq_Database* DB) {
    vector<string> *keys = new vector<string>(); // list keys
    vector<unordered_map<string,string>> ins_vec = segmentData();
    DB->insert("games", ins_vec);
    return true;
}

bool testInsert(Seq_Database* DB, int range) { // insert one record
    vector<string> *keys = new vector<string>(); // list keys
    vector<unordered_map<string,string>> ins_vec = segmentData();
    vector<tuple<string,string,int>> *empt = new vector<tuple<string,string,int>>();
    vector<unordered_map<string,string>> res1 = DB->get("games", *empt);
    DB->insert("games", ins_vec);
    vector<unordered_map<string,string>> res2 = DB->get("games", *empt);
    cout << res1.size() << endl;
    cout << res2.size() << endl;
    return true;
}

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

bool testRemove(Seq_Database *DB, tuple<string,string, int> c1) { // singular remove
    vector<tuple<string, string, int>>* conds = new vector<tuple<string, string, int>>();
    conds->push_back(c1);
    DB->remove("games", *conds);
    cout << "POST:" << endl;
    vector<unordered_map<string, string>> results = DB->get("games", *conds);
    for (unordered_map<string,string> x : results){
        for(auto y : x){
            cout << y.first << ": " << y.second << ", ";
        }
        cout << endl << endl;
    }
    return true;
}

vector<unordered_map<string,string>> getRecords(Seq_Database* DB, int range) {
    random_device rd; 
    mt19937 gen(rd()); 
    uniform_int_distribution<> distr(1, 500);
    vector<unordered_map<string,string>> ins_vec = segmentData();
    int rand_val = distr(gen);
    vector<unordered_map<string,string>> sliced_vec = vector<unordered_map<string,string>>((ins_vec.begin() + rand_val), (ins_vec.begin() + rand_val + range));
    return sliced_vec;
}

bool testRigorous(Seq_Database* DB, int iters) {
    chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    // // test 250 inserts
    // testInsert(DB, 50);
    // testInsert(DB, 50);
    // testInsert(DB, 50);
    // testInsert(DB, 50);
    // testInsert(DB, 50);
    
    // tuple<string, string, int> rm1 = make_tuple<string, string, int>("team_home", "New England Patriots", 0);
    // tuple<string, string, int> rm2 = make_tuple<string, string, int>("team_home", "Philadelphia Eagles", 0);
    // tuple<string, string, int> rm3 = make_tuple<string, string, int>("team_home", "Miami Dolphins", 0);
    // tuple<string, string, int> rm4 = make_tuple<string, string, int>("team_home", "Houston Oilers", 0);

    // // test removes
    // testRemove(DB, rm1);
    // testRemove(DB, rm2);
    // testRemove(DB, rm3);
    // testRemove(DB, rm4);

    vector<unordered_map<string,string>> rand_rows = getRecords(DB, 50);

    for( int i = 0; i < iters; i++ ) {
        cout << i << endl;
        DB->insert("games", rand_rows);
    }

    // test GET
    testGet(DB);
    chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[µs]" << std::endl;
  
    return true;
}

int main() {
    Seq_Database* DB = new Seq_Database(); // testing sequential
    populateDB(DB);

    // ----- insert ------
    // test random insert
    // testInsert(DB, 50);
    // -------------------

    // ----- get ---------
    //testGet(DB);
    // -------------------

    // ----- remove ------
    // tuple<string, string, int> rm = make_tuple<string, string, int>("team_home", "New England Patriots", 0);
    // testRemove(DB, rm);
    // -------------------

    // ----- BATCH OPS ---
    testRigorous(DB, 50);
    // -------------------


    exit(EXIT_SUCCESS);
}
