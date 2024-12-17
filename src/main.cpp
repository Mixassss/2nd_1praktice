#include "../include/ht.h"
#include "../include/list.h"
#include "../include/commands.h"
#include "../include/json.hpp"

void BaseData::parser() {
    nlohmann::json Json;
    ifstream fin("../base_date.json");
    if (!fin.is_open()) {
        cerr << "Ошибка! Не удалось открыть файл Json!" << endl;
        exit(1);
    }
    fin >> Json;
    BD = Json["name"].get<string>();
    rowLimits = Json["tuples limit"].get<int>();

    if (Json.contains("structure") && Json["structure"].is_object()) {
        for (auto& item : Json["structure"].items()) {
            string tableName = item.key();
            tablesname.pushBack(tableName);

            // Сбор колонок с первичным ключом
            string columnString = ""; // Обнуляем строку
            for (auto& column : item.value().items()) {
                columnString += column.value().get<string>() + ",";
            }
            columnString = tableName + "_pk," + columnString; // Добавляем первичный ключ
            if (!columnString.empty()) columnString.pop_back(); // Убираем последний запятую
            coloumnHash.insert(tableName, columnString);
            fileCountHash.insert(tableName, 1);
        }
    } else {
        cerr << "Структура не найдена!" << endl;
        exit(1);
    }
}

void BaseData::createdirect() {
    string baseDir = "../" + BD;
    filesystem::create_directories(baseDir);
    for (int i = 0; i < tablesname.elementCount; ++i) {
        string tableName = tablesname.getElementAt(i);
        string tableDir = baseDir + "/" + tableName;
        filesystem::create_directories(tableDir);

        string columnString;
        if (!coloumnHash.get(tableName, columnString)) {
            cerr << "Не удалось получить столбцы для таблицы: " << tableName << endl;
            continue;
        }
        string csvFilePath = tableDir + "/1.csv";
        ofstream csvFile(csvFilePath);
        if (csvFile.is_open()) {
            csvFile << columnString << endl; 
            csvFile.close();
        }
        createLockFile(tableDir, tableName);
        createPKFile(tableDir, tableName);
    }
}

void BaseData::createLockFile(const string& dir, const string& tableName) {
    ofstream lockFile(dir + "/" + tableName + "_lock.txt");
    lockFile << "open";
}

void BaseData::createPKFile(const string& dir, const string& tableName) {
    ofstream pkFile(dir + "/" + tableName + "_pk_sequence.txt");
    if (pkFile.is_open()) {
        pkFile << "1"; 
        pkFile.close();
    } else {
        cerr << "Ошибка! Не удалось открыть файл для записи! " << endl;
    }
}

void BaseData::commands(string& command) {
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
        airport.commands(command);
    }

    return 0;
}