#include <string>
#include <stdlib.h>
#include <stdio.h >

using namespace std;

class DP_Database{
    private:
    unordered_hashmap<string,void*> DB;

    public:

    bool insert(string table, int num, void* records);
    bool remove(string table, int num, void* records);
    bool get(string table, void* conditions, void* sort, bool ascend = true);
    
    
    

}