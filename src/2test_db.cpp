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
#include <tbb/tbb.h>
#include "DataParallelDB.cpp"

using namespace std;

vector<unordered_map<string,string>> segmentData() {
    chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

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

int populateDB(Seq_Database* DB) {
    chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    vector<string> *keys = new vector<string>(); // list keys
    vector<unordered_map<string,string>> ins_vec = segmentData();
    cout << "inserting" << endl;
    DB->insert("games", ins_vec);

    chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    int ms = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    std::cout << "Populate Data Time = " << ms << "[??s]" << std::endl;

    return ms;
}

int populateDB(DP_Database* DB) {
    chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    vector<string> *keys = new vector<string>(); // list keys
    vector<unordered_map<string,string>> ins_vec = segmentData();
    cout << "inserting" << endl;
    DB->insert("games", ins_vec);

    chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    int ms = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    std::cout << "Populate Data Time = " << ms << "[??s]" << std::endl;

    return ms;
}

int testInsert(Seq_Database* DB, int range) { // insert one record

    vector<string> *keys = new vector<string>(); // list keys
    vector<unordered_map<string,string>> ins_vec = segmentData();
    chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    vector<tuple<string,string,int>> *empt = new vector<tuple<string,string,int>>();
    vector<unordered_map<string,string>> sliced = {ins_vec.begin() + 1, ins_vec.end() - ins_vec.size() + range};

//    vector<unordered_map<string,string>> res1 = DB->get("games", *empt);
    DB->insert("games", sliced);
//    vector<unordered_map<string,string>> res2 = DB->get("games", *empt);
//    cout << res1.size() << endl;
//    cout << res2.size() << endl;
    chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    std::cout << "Insertion Time = " << ms << "[??s]" << std::endl;

    return int(ms);
}

int testInsert(DP_Database* DB, int range) { // insert one record

    vector<string> *keys = new vector<string>(); // list keys
    vector<unordered_map<string,string>> ins_vec = segmentData();
    chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    vector<tuple<string,string,int>> *empt = new vector<tuple<string,string,int>>();
    vector<unordered_map<string,string>> sliced = {ins_vec.begin() + 1, ins_vec.end() - ins_vec.size() + range};

//    vector<unordered_map<string,string>> res1 = DB->get("games", *empt);
    DB->insert("games", sliced);
//    vector<unordered_map<string,string>> res2 = DB->get("games", *empt);
//    cout << res1.size() << endl;
//    cout << res2.size() << endl;
    chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    std::cout << "Insertion Time = " << ms << "[??s]" << std::endl;

    return int(ms);
}

bool testGet(Seq_Database* DB) { // c1, c2 = criteria
    chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

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

//    tbb::concurrent_vector<unordered_map<string, string>> results = DB->get("games", *conds3);
    vector<unordered_map<string, string>> results = DB->get("games", *conds);

    for (unordered_map<string,string> x : results){
        for(auto y : x){
            cout << y.first << ": " << y.second << ", ";
        }
        cout << endl << endl;
    }
    cout << "Total records: " << results.size() << endl;

    chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "Get Time = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[??s]" << std::endl;

    return true;
}

bool testGet(DP_Database* DB) { // c1, c2 = criteria
    chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

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

//    tbb::concurrent_vector<unordered_map<string, string>> results = DB->get("games", *conds3);
    tbb::concurrent_vector<unordered_map<string, string>> results = DB->get("games", *conds);

//    for (unordered_map<string,string> x : results){
//        for(auto y : x){
//            cout << y.first << ": " << y.second << ", ";
//        }
//        cout << endl << endl;
//    }
//    cout << "Total records: " << results.size() << endl;

    chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "Get Time = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[??s]" << std::endl;

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

bool testRemove(DP_Database *DB, tuple<string,string, int> c1) { // singular remove
    vector<tuple<string, string, int>>* conds = new vector<tuple<string, string, int>>();
    conds->push_back(c1);
    int r = DB->remove("games", *conds);
    cout << "POST:"<< r << endl;
    tbb::concurrent_vector<unordered_map<string, string>> results = DB->get("games", *conds);
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

vector<string> fetchNFLTeams(Seq_Database*DB) {
    std::ifstream file("../data/nfl_teams.csv"); // import csv
    vector<string> teams; // vec of games
    string s = ""; // for tracking label rows
    char delim = '\n'; // delimiter by newline

    std::string row;

    while (std::getline(file, row, delim)) {
        teams.push_back(row); // vector of rows, raw
    }

    return teams;
}

bool testRigorous(Seq_Database* DB, int iters) {
    chrono::steady_clock::time_point outer_begin = std::chrono::steady_clock::now();

    vector<unordered_map<string,string>> rand_rows = getRecords(DB, 1000);

    // test INSERT
    // ---------------
    chrono::steady_clock::time_point begin1 = std::chrono::steady_clock::now();
    for( int i = 0; i < iters; i++ ) {
        DB->insert("games", rand_rows);
    }
    chrono::steady_clock::time_point end1 = std::chrono::steady_clock::now();
    std::cout << "Rigorous INSERT of " << iters << " iterations= " << std::chrono::duration_cast<std::chrono::microseconds>(end1 - begin1).count() << "[??s]" << std::endl;

    // ---------------

    // test GET
    // ---------------
    vector<string> team_list = fetchNFLTeams(DB);
    string team = team_list.at(rand() % 30 + 1);
    chrono::steady_clock::time_point begin2 = std::chrono::steady_clock::now();

    for( int i = 0; i < iters; i++ ) { // test random home teams
        tuple<string, string, int> home = make_tuple<string, string>("team_home", string(team), 0);
        vector <tuple<string, string, int>> *conds = new vector <tuple<string, string, int>>();
        conds->push_back(home);
        DB->get("games", *conds);
    }

    chrono::steady_clock::time_point end2 = std::chrono::steady_clock::now();
    std::cout << "Rigorous GET of " << iters << " iterations= " << std::chrono::duration_cast<std::chrono::microseconds>(end2 - begin2).count() << "[??s]" << std::endl;


    // ---------------


    chrono::steady_clock::time_point outer_end = std::chrono::steady_clock::now();
    std::cout << "TEST RIGOROUS TOTAL = " << std::chrono::duration_cast<std::chrono::microseconds>(outer_end - outer_begin).count() << "[??s]" << std::endl;

    return true;
}

void test_insert_speeds(Seq_Database* DB, DP_Database* dpDB, int iters) {
    vector<unordered_map<string,string>> records = getRecords(DB, 10000);
    vector<unordered_map<string,string>> records1 = getRecords(DB, 10000);

    for (int j = 0; j < iters; j++){
        auto old_count = records.size();
        records.resize(records1.size() + old_count);
        std::copy_n(records1.begin(), records1.size(), records.begin() + old_count);
    }

    chrono::steady_clock::time_point ibegin1 = std::chrono::steady_clock::now();
    DB->insert("games", records);
    chrono::steady_clock::time_point iend1 = std::chrono::steady_clock::now();
    std::cout << "Sequential Insert Time = " << std::chrono::duration_cast<std::chrono::microseconds>(iend1 - ibegin1).count() << "[??s]" << std::endl;

    chrono::steady_clock::time_point ibegin = std::chrono::steady_clock::now();
    dpDB->insert("games", records);
    chrono::steady_clock::time_point iend = std::chrono::steady_clock::now();
    std::cout << "Parallel Insert Time = " << std::chrono::duration_cast<std::chrono::microseconds>(iend - ibegin).count() << "[??s]" << std::endl;
}

void test_get_speeds(Seq_Database* DB, DP_Database* dpDB) {
    tuple<string, string, int> home = make_tuple<string, string>("team_away", "New England Patriots", 0);
    tuple<string, string, int> score = make_tuple<string, string>("score_home", "14",1);
    tuple<string, string, int> stadium = make_tuple<string, string>("stadium", "Mile High", -1);
    vector<tuple<string, string, int>>* conds = new vector<tuple<string, string, int>>();

    conds->push_back(home);
    conds->push_back(score);
    conds->push_back(stadium);

    chrono::steady_clock::time_point ibegin1 = std::chrono::steady_clock::now();
    DB->get("games", *conds);
    chrono::steady_clock::time_point iend1 = std::chrono::steady_clock::now();
    std::cout << "Sequential GET Time = " << std::chrono::duration_cast<std::chrono::microseconds>(iend1 - ibegin1).count() << "[??s]" << std::endl;

    chrono::steady_clock::time_point ibegin = std::chrono::steady_clock::now();
    dpDB->get("games", *conds);
    chrono::steady_clock::time_point iend = std::chrono::steady_clock::now();
    std::cout << "Parallel GET Time = " << std::chrono::duration_cast<std::chrono::microseconds>(iend - ibegin).count() << "[??s]" << std::endl;
}

int main() {
    Seq_Database* DB = new Seq_Database(); // testing sequential
    DP_Database* dpDB = new DP_Database();
    populateDB(DB);
    populateDB(dpDB);

//    testGet(DB);
//    testGet(dpDB);

    // ----- insert ------
    // test random insert
//    test_insert_speeds(DB, dpDB, 50);
    // -------------------

    // ----- get ---------
//    testGet(dpDB);
     test_get_speeds(DB, dpDB);
    // -------------------

    // ----- remove ------
    // tuple<string, string, int> rm = make_tuple<string, string, int>("team_home", "New England Patriots", 0);
    // testRemove(dpDB, rm);
    // -------------------

    // ----- BATCH OPS ---
//     testRigorous(DB, 50);
    // -------------------

    // ---- Test Clean ----
    tuple<string, string, int> rm = make_tuple<string, string, int>("team_home", "New England Patriots", 0);
    testRemove(dpDB, rm);
    dpDB->cleanTable("games");
    dpDB->printDB();

    exit(EXIT_SUCCESS);
}
