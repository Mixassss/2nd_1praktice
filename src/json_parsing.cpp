#include "../include/json_parsing.h"

struct json {
    string name;
    int tuples_limit;
    Structure structure;
};

struct Structure {
    string* heroes[3];
    char* statistics[2];
};

void ReadFile(const string& filepath, string& output) {
    ifstream file(filepath);
    string line;

    while(getline(file, line)) {
        output.append(line);
    }
}