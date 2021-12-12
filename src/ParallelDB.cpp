#include <string>
#include <stdlib>
#include <stdio>

using namespace std;

class Database{
    private:
    unordered_hashmap<string,void*> DB;

    public:

    bool insert_seq(string table, int num, void* records); 
    bool insert_data_parallel(string table, int num, void* records);

    bool remove_seq(string table, int num, void* records);
    bool remove_data_parallel(string table, int num, void* records);

    bool get_seq(string table, void* conditions, void* sort, bool ascend = true); 
    bool get_data_parallel(string table, void* conditions, void* sort, bool ascend = true);
    bool get_task_parallel(string table, void* condistions, void* sort, bool ascend = true);
    bool get_task_data(string table, void* condistions, void* sort, bool ascend = true);
    
}