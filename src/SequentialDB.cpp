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

using namespace std;

class Seq_Database {
    private:
    unordered_map<string, unordered_map<string,void*>>* DB;

    class compObj{
        private:
        string sortKey;
        bool ascend = true;
        public:
        compObj(string sort, bool asc){
            sortKey = sort;
            ascend = asc;
        }
        bool operator()(void* A, void* B){
            unordered_map<string, void*> X = *((unordered_map<string, void*>*) A);
            unordered_map<string, void*> Y = *((unordered_map<string, void*>*) B);
            if(X[sortKey] < Y[sortKey]){
                if(ascend){
                    return true;
                }
                return false;
            }
            if(ascend){
                return false;
            }
            return true;
        }
    };

    public:

    Seq_Database(){
        DB = new unordered_map<string, unordered_map<string,void*>>();
    }

    bool insert(string table, int num, vector<string> keys, vector<void*> records){
        try {
            DB->at(table);
        } catch (const std::out_of_range& oor) {
            unordered_map<string, void*> database = *DB;
            database[table] = new unordered_map<string, void*>();
            //DB->insert(std::make_pair<string, unordered_map<string, void*>(table, new unordered_map<string, void*>());
        }
        for(int i = 0; i < num; i++){
            DB[table][keys[i]] = records[i];
        }
    }

    /*
    // remove specific records
    int remove(string table, vector<string> keys){
        int failed = 0;
        for(int i = 0; i < keys.size(); i++){
            if(!DB[table].erase(keys[i])){
                failed++;
            }
        }
        return failed;
    }

    // remove based on conditions
    int remove(string table, int numCond, vector<void*> conditions){
        int count = 0;
        vector<string> toRemove;
        for(x : DB[table]){
            remove = true;
            for(int i = 0; i < numCond; i++){
                y = (unordered_map<string,void*>) x->second;
                remove = remove && y[conditions[i][0]]==conditions[i][1]];
            }
            if(remove){
                count++;
                toRemove.push_back(x->first);
            }
        }
        return count - remove(table, toRemove);
    }

    // Qry without sort
    vector<void*> get(string table, vector<void*> conditions){
        int count = 0;
        vector<void*> finalQry = new vector<void*>();
        for(x : DB[table]) {
            add = true;
            for(int i = 0; i < numCond; i++) {
                y = (unordered_map<string,void*>) x->second;
                add = add && y[conditions[i][0]]==conditions[i][1]];
            }
            if(add) {
                count++;
                finalQry.push_back(x->second);
            }
        };
        return finalQry;
    }

    // bool compAsc(void* a, void* b, void* key){
    //     sortKey = a["sort"];
    //     if(a[sortKey] < b[sortKey]){
    //         return true;
    //     }
    //     return false;
    // }

    // bool compDesc(void* a, void* b){
    //     sortKey = a["sort"];
    //     if(a[sortKey] < b[sortKey]){
    //         return false;
    //     }
    //     return true;
    // }

    // Qry with sort
    vector<void*> get(string table, vector<void*> conditions, void* sort, bool ascend = true){
        int count = 0;
        vector<void*> finalQry = new vector<void*>();
        for(x : DB[table]){
            add = true;
            for(int i = 0; i < numCond; i++){
                y = (unordered_map<string,void*>) x->second;
                add = add && y[conditions[i][0]]==conditions[i][1];
            }
            if(add){
                count++;
                string s = "sort";
                ((unordered_map<string,void*>) x->second)["sort"] = sort;
                finalQry.push_back(x->second);
            }
        }
        compObj comparator(sort, ascend);
        if (ascend){
            sort(finalQry.begin(), finalQry.end(), compObj);
            return finalQry;
        }
        sort(finalQry.begin(), finalQry.end(), compObj);
        return finalQry;
    }
*/
};