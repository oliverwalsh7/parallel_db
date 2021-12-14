/**
* @authors: Vasilios Walsh & Justin Angelel
* CSE F21-475 Principles and Practices of Parallel Computation
* Professor Palmieri
* December 2021
**/

#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <algorithm> // sort
#include <unordered_map>
#include <vector>
#include <iostream>

using namespace std;

class Seq_Database {
    private:
    // Outer Level Hashmap of tables
    //   Second Level Hashmap of records in "table"
    //     Third Level Hashmap where each map is a record
    unordered_map<string, vector<unordered_map<string,string>>>* DB;
    //unordered_map<string, unordered_map<string, unordered_map<string,void*>>>* DB;
    //string sortKey;

    // class compObj{
    //     private:
    //     string sortKey;
    //     bool ascend = true;
    //     public:
    //     compObj(string sort, bool asc){
    //         sortKey = sort;
    //         ascend = asc;
    //     }
    //     bool operator()(unordered_map<string, string> A, unordered_map<string, string> B){
    //         unordered_map<string, void*> X = *((unordered_map<string, void*>*) A);
    //         unordered_map<string, void*> Y = *((unordered_map<string, void*>*) B);
    //         if(X[sortKey] < Y[sortKey]){
    //             if(ascend){
    //                 return true;
    //             }
    //             return false;
    //         }
    //         if(ascend){
    //             return false;
    //         }
    //         return true;
    //     }
    // };

    public:

    Seq_Database(){
        DB = new unordered_map<string, vector<unordered_map<string,string>>>();
    }

    void insert(string table, vector<unordered_map<string,string>> records){
        try {
            DB->at(table);
        } catch (const std::out_of_range& oor) {
            vector<unordered_map<string,string>>* x = new vector<unordered_map<string,string>>();
            (*DB)[table] = *x;
            cout << "Table doesnt exist" << endl;
        }
        for(int i = 0; i < records.size(); i++){
            (*DB)[table].push_back(records[i]);
        }

    }

    // remove based on conditions
    int remove(string table, vector<tuple<string,string, int>> conditions){
        int count = 0;
        for(auto it = (*DB)[table].begin(); it != (*DB)[table].end(); it++) { // for every unordered_map (record) in the vector representing "table"
            bool rem = true;
            for(int i = 0; i < conditions.size(); i++) { // check each condition
                string k = std::get<0>(conditions[i]);
                string field = (*it)[k];
                if(std::get<2>(conditions[i])<0){
                    rem = rem && (field.compare(std::get<1>(conditions[i])) < 0); 
                } else if(std::get<2>(conditions[i])>0){
                    rem = rem && (field.compare(std::get<1>(conditions[i])) > 0);
                } else {
                    rem = rem && (field.compare(std::get<1>(conditions[i])) == 0);
                }            
            }
            if(rem) {
                count++;
                it = (*DB)[table].erase(it);
                it--;
            }
        }
        return count;
    } 

    // Qry without sort
    vector<unordered_map<string,string>> get(string table, vector<tuple<string,string, int>> conditions){
        int count = 0;
        vector<unordered_map<string, string>>* finalQry = new vector<unordered_map<string, string>>();
        for(auto x : (*DB)[table]) { // for every unordered_map (record) in the vector representing "table"
            bool add = true;
            for(int i = 0; i < conditions.size(); i++) { // check each condition
                string k = std::get<0>(conditions[i]);
                string field = ((unordered_map<string, string>)x)[k];
                if(std::get<2>(conditions[i])<0){
                    add = add && (field.compare(std::get<1>(conditions[i])) < 0); 
                } else if(std::get<2>(conditions[i])>0){
                    add = add && (field.compare(std::get<1>(conditions[i])) > 0);
                } else {
                    add = add && (field.compare(std::get<1>(conditions[i])) == 0);
                }

            }
            if(add) {
                count++;
                finalQry->push_back(x);
            }
        }
        return *finalQry;
    }

    /*static bool compAsc(unordered_map<string, void*> a, unordered_map<string, void*> b){ //IS THIS RIGHT? NEED TO DEREFERENCE?
        string sortKey = *((unordered_map<string, void*>)a)["sort"];
        if(a[sortKey] < b[sortKey]){
            return true;
        }
        return false;
    }

    static bool compDesc(unordered_map<string, void*> a, unordered_map<string, void*> b){ //IS THIS RIGHT? NEED TO DEREFERENCE?
        if(a[sortKey] < b[sortKey]){
            return false;
        }
        return true;
    }

    // Qry with sort
    vector<unordered_map<string, void*>> get(string table, vector<pair<string,void*>> conditions, string sort, bool ascend = true){
        // int count = 0;
        // vector<void*>* finalQry = new vector<void*>();
        // for(auto x : DB[table]){
        //     add = true;
        //     for(int i = 0; i < conditions.size(); i++){
        //         y = (unordered_map<string,void*>) x->second;
        //         add = add && y[conditions[i][0]]==conditions[i][1];
        //     }
        //     if(add){
        //         count++;
        //         //string s = "sort";
        //         //IF THE COMP OBJECT WORKS THIS IS NOT NECESSARY
        //         //((unordered_map<string,void*>) x->second)["sort"] = sort;
        //         finalQry.push_back(x->second);
        //     }
        // }
        int count = 0;
        vector<unordered_map<string,void*>>* finalQry = new vector<unordered_map<string,void*>>();
        unordered_map<string,unordered_map<string, void*>> database = *DB;
        for(auto x : database[table]) {
            bool add = true;
            for(int i = 0; i < conditions.size(); i++) {
                auto field = (*((unordered_map<string, void*>*)x.second))[conditions[i].first];
                add = add && field == conditions[i].second; // THIS IS NOT RIGHT, NEED TO CASTE TO PROPER TYPES THEN DEREFERENCE
            }
            if(add) {
                count++;
                string s = "sort";
                //IF THE COMP OBJECT WORKS THIS IS NOT NECESSARY
                (*((unordered_map<string, void*>*)x.second))["sort"] = sort;
                finalQry->push_back((*((unordered_map<string, void*>*)x.second)));
            }
        }
        //vector<unordered_map<string, void*>> finalQry = get(table, conditions);

        // compObj comparator(sort, ascend);
        //sortKey = sort;
        if (ascend){
            sort(finalQry.begin(), finalQry.end(), compAsc);
            return finalQry;
        }
        sort(finalQry.begin(), finalQry.end(), compDesc);
        return finalQry;
    }*/

    void printDB(){
        unordered_map<string,vector<unordered_map<string,string>>> database = *DB;
        cout << "print all the rows in DB" << endl;
        cout << database.size() << endl;
        for(auto x : database){ // outer loop of tables
            cout << x.second.size() << endl;
            //cout << x.second.bucket_count();
            for(auto y : x.second){ // middle loop of records
                for(auto z : y){ // inner loop of fields
                    cout << z.second << " ";
                }
                cout << endl<< endl;
            }
        }
    }

};