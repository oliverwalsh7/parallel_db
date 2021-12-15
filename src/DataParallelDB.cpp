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
        try {
            DB->at(table);
        } catch (const std::out_of_range& oor) {
            //table does not exist, so make one
            tbb::concurrent_vector<unordered_map<string,string>>* x = new tbb::concurrent_vector<unordered_map<string,string>>();
            //database.insert(make_pair(table,*x));
            (*DB).insert(make_pair(table,*x));
        }
        tbb::parallel_for( tbb::blocked_range<int>(0,records.size()), //Loop in parallel over all records to be inserted
                       [&](tbb::blocked_range<int> r)
        {
            for (int i=r.begin(); i<r.end(); ++i)
            {
                records[i]["Valid"] = "True";
                (*DB)[table].push_back(records[i]); // concurrently append to vector. tbb::concurrent vector is safe for resizes and appends operations
            }
        });
    }

    void insertV2(string table, vector<unordered_map<string,string>> records){
        try {
            DB->at(table);
        } catch (const std::out_of_range& oor) {
            //table does not exist, so make one
            tbb::concurrent_vector<unordered_map<string,string>>* x = new tbb::concurrent_vector<unordered_map<string,string>>();
            //database.insert(make_pair(table,*x));
            (*DB).insert(make_pair(table,*x));
        }
        int old_size = (*DB)[table].size();
        (*DB)[table].grow_to_at_least(old_size+records.size());
        tbb::parallel_for( tbb::blocked_range<int>(0,records.size()), //Loop in parallel over all records to be inserted
                       [&](tbb::blocked_range<int> r)
        {
            for (int i=r.begin(); i<r.end(); ++i)
            {
                records[i]["Valid"] = "True";
                (*DB)[table][old_size+i] = records[i]; // concurrently append to vector. tbb::concurrent vector is safe for resizes and appends operations
            }
        });
    }

    tbb::concurrent_vector<unordered_map<string,string>> get(string table, vector<tuple<string,string, int>> conditions){
        int count = 0;
        tbb::concurrent_vector<unordered_map<string, string>>* finalQry = new tbb::concurrent_vector<unordered_map<string, string>>();
        tbb::parallel_for( tbb::blocked_range<int>(0,(*DB)[table].size()), //Loop in parallel over all records to be inserted
                       [&](tbb::blocked_range<int> r)
        {
            for (int i=r.begin(); i<r.end(); ++i)
            {
                bool add = true;
                for(int j = 0; j < conditions.size(); j++) { // check each condition
                    string k = std::get<0>(conditions[j]);
                    //string field = ((unordered_map<string, string>)x)[k];
                    string field = (*DB)[table][i][k];
                    if(std::get<2>(conditions[j])<0){
                        add = add && (field.compare(std::get<1>(conditions[j])) < 0); 
                    } else if(std::get<2>(conditions[j])>0){
                        add = add && (field.compare(std::get<1>(conditions[j])) > 0);
                    } else {
                        add = add && (field.compare(std::get<1>(conditions[j])) == 0);
                    }

                }
                if(add && (*DB)[table][i]["Valid"].compare("True") == 0) {
                    count++;
                    finalQry->push_back((*DB)[table][i]); // concurrently append to vector. tbb::concurrent vector is safe for resizes and appends operations
                } 
            }
        });
        return *finalQry;
    }

    int remove(string table, vector<tuple<string,string, int>> conditions){
        int count = 0;
        tbb::parallel_for( tbb::blocked_range<int>(0,(*DB)[table].size()), //Loop in parallel over all records to be inserted
                       [&](tbb::blocked_range<int> r)
        {
            for (int i=r.begin(); i<r.end(); ++i)
            {
             // for every unordered_map (record) in the vector representing "table"
                bool rem = true;
                for(int j = 0; j < conditions.size(); j++) { // check each condition
                    string k = std::get<0>(conditions[j]);
                    string field = (*DB)[table][i][k];
                    if(std::get<2>(conditions[j])<0){
                        rem = rem && (field.compare(std::get<1>(conditions[j])) < 0); 
                    } else if(std::get<2>(conditions[j])>0){
                        rem = rem && (field.compare(std::get<1>(conditions[j])) > 0);
                    } else {
                        rem = rem && (field.compare(std::get<1>(conditions[j])) == 0);
                    }            
                }
                if(rem) {
                    count++;
                    (*DB)[table][i]["Valid"] = "False";
                }
            }
        });
        return count;
    } 

    void cleanTable(string table){
        tbb::concurrent_vector<unordered_map<string, string>>* newTable = new tbb::concurrent_vector<unordered_map<string, string>>();
        for(int i = 0; i < (*DB)[table].size(); i++){
            if((*DB)[table][i]["Valid"].compare("True") == 0){
                newTable->push_back((*DB)[table][i]);
            }
        }
        (*DB)[table]=move(*newTable);
    }

    void printDB(){
        cout << "print all the rows in DB" << endl;
        cout << (*DB).size() << endl;
        int count = 0;
        for(auto x : (*DB)){ // outer loop of tables
            cout << x.second.size() << endl;
            //cout << x.second.bucket_count();
            for(auto y : x.second){ // middle loop of records
                for(auto z : y){ // inner loop of fields
                    cout << z.second << " ";
                }
                if(y["Valid"].compare("False") == 0){
                    count++;
                }
                cout << endl<< endl;
            }
        }
        cout << count << endl;
    }
    
};