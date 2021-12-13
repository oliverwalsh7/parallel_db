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

    void insert(string table, vector<unordered_map<string,string>> records){
        unordered_map<string,tbb::concurrent_vector<unordered_map<string,string>>> database = *DB;
        try {
            DB->at(table);
        } catch (const std::out_of_range& oor) {
            //table does not exist, so make one
            tbb::concurrent_vector<unordered_map<string,string>>* x = new tbb::concurrent_vector<unordered_map<string,string>>();
            //database.insert(make_pair(table,*x));
            database.insert(make_pair(table,*x));
        }
        tbb::parallel_for( tbb::blocked_range<int>(0,records.size()), //Loop in parallel over all records to be inserted
                       [&](tbb::blocked_range<int> r)
        {
            for (int i=r.begin(); i<r.end(); ++i)
            {
                database[table].push_back(records[i]); // concurrently append to vector. tbb::concurrent vector is safe for resizes and appends operations
            }
        });
        *DB = database;
    }

    tbb::concurrent_vector<unordered_map<string,string>> get(string table, vector<tuple<string,string, int>> conditions){
        int count = 0;
        tbb::concurrent_vector<unordered_map<string, string>>* finalQry = new tbb::concurrent_vector<unordered_map<string, string>>();
        unordered_map<string,tbb::concurrent_vector<unordered_map<string,string>>> database = *DB;
        tbb::parallel_for( tbb::blocked_range<int>(0,database[table].size()), //Loop in parallel over all records to be inserted
                       [&](tbb::blocked_range<int> r)
        {
            for (int i=r.begin(); i<r.end(); ++i)
            {
                bool add = true;
                for(int j = 0; j < conditions.size(); j++) { // check each condition
                    string k = std::get<0>(conditions[j]);
                    //string field = ((unordered_map<string, string>)x)[k];
                    string field = database[table][i][k];
                    if(std::get<2>(conditions[j])<0){
                        add = add && (field.compare(std::get<1>(conditions[j])) < 0); 
                    } else if(std::get<2>(conditions[j])>0){
                        add = add && (field.compare(std::get<1>(conditions[j])) > 0);
                    } else {
                        add = add && (field.compare(std::get<1>(conditions[j])) == 0);
                    }

                }
                if(add) {
                    count++;
                    finalQry->push_back(database[table][i]); // concurrently append to vector. tbb::concurrent vector is safe for resizes and appends operations
                } 
            }
        });
        return *finalQry;
    }
    
};