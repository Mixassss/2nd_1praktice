#include "../include/ht.h"
#include "../include/list.h"
#include "../include/commands.h"
#include "../include/json.hpp"

int countingLine(string& fin) { // ф-ия подсчёта строк в файле
    ifstream file;
    file.open(fin);
    int countline = 0;
    string line;
    while(getline(file, line)) {
        countline++;
    }
    file.close();
    return countline;
}

string fileread(string& filepath) { // чтение из файла
    string result, str;
    ifstream fileinput;
    fileinput.open(filepath);
    while (getline(fileinput, str)) {
        result += str + '\n';
    }
    result.pop_back();
    fileinput.close();
    return result;
}

void filerec(string& filename, string data) { // запись в файл
    ofstream fileoutput;
    fileoutput.open(filename);
    fileoutput << data;
    fileoutput.close();
}

void BaseData::checkcommand(string& command) {
    if (command.substr(0, 11) == "INSERT INTO") {
        command.erase(0, 12);
        Insert(command);
    } else if (command.substr(0, 11) == "DELETE FROM") {
        command.erase(0, 12);
        Delete(command);
    } else if (command.substr(0, 6) == "SELECT") {
        command.erase(0, 7);
        isValidSelect(command);
    } else if (command == "STOP") {
        exit(0);
    } else {
        cout << "Ошибка, неизвестная команда!" << endl; 
    }
}

int main() {
    BaseData airport;

    airport.parser();
    airport.createdirect();
    string command;
    while (true) {
        cout << endl << "Вводите команду: ";
        getline(cin, command);
        airport.checkcommand(command);
    }

    return 0;
}