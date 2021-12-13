#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <algorithm> // sort
#include <unordered_map>
#include <vector>
#include <iostream>
#include <tbb/tbb.h>

using namespace std;

class DP_Database{
    private:
    unordered_map<string, tbb::concurrent_vector<unordered_map<string,string>>>* DB;

    public:

    DP_Database(){
        DB = new unordered_map<string, tbb::concurrent_vector<unordered_map<string,string>>>();
    }

    void insert(string table, int num, vector<string> keys, vector<unordered_map<string,string>> records){
        unordered_map<string,tbb::concurrent_vector<unordered_map<string,string>>> database = *DB;
        try {
            DB->at(table);
        } catch (const std::out_of_range& oor) {
            //table does not exist, so make one
            tbb::concurrent_vector<unordered_map<string,string>>* x = new tbb::concurrent_vector<unordered_map<string,string>>();
            //database.insert(make_pair(table,*x));
            database.insert(make_pair(table,*x));
        }
        tbb::parallel_for( tbb::blocked_range<int>(0,records.size()),
                       [&](tbb::blocked_range<int> r)
        {
            for (int i=r.begin(); i<r.end(); ++i)
            {
                database[table].push_back(records[i]);
            }
        });
        *DB = database;
    }
    
};