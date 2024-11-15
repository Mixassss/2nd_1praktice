#include "../include/json.hpp"
#include "../include/list.h"
#include "../include/ht.h"

string fileread (string& filename) { // Производим чтение из файла
    string result, str;
    ifstream fin(filename);
    while (getline(fin, str)) {
        result += str + "\n";
    }
    result.pop_back();
    fin.close();
    return result;
}

void filerec (string& filename, string& data) { // Производим запись в файл
    ofstream fout(filename);
    fout << data;
    fout.close();
}

int countingLine (string& filename) {
    int linecout = 0;
    string str;
    ifstream fin(filename);
    while (getline(fin, str)) {
        linecout++; // Увеличиваем счетчик
    }
    fin.close();
    return linecout;
}