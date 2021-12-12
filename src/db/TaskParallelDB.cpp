#include <string>
#include <stdlib.h>
#include <stdio.h>

using namespace std;

class DP_Database{
    private:
    unordered_hashmap<string,void*> DB;

    public:
    bool get_task_parallel(string table, void* condistions, void* sort, bool ascend = true);
    bool get_task_data(string table, void* condistions, void* sort, bool ascend = true);

}