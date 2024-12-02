#include "../include/json.hpp"
#include "../include/list.h"
#include "../include/ht.h"

string fileread (const string& filename) { // Производим чтение из файла
    string result, line;
    ifstream fin(filename);
    if(!fin.is_open()) {
        cerr << "Ошибка! Не удается открыть файл" << filename << endl;
        return "";
    }
    while (getline(fin, line)) {
        result += line + "\n";
    }
    if (!result.empty()) {
        result.pop_back(); // удаляем последний символ новой строки
    }
    fin.close();
    return result;
}

void filerec (const string& filename, const string& data) { // Производим запись в файл
    ofstream fout(filename);
    if (!fout.is_open()) {
        cerr << "Ошибка! Не удается открыть файл для записи" << filename << endl;
        return;
    }
    fout << data << endl;
    fout.close();
}

int countingLine (const string& filename) {
    int linecout = 0;
    string str;
    ifstream fin(filename);
    if (!fin.is_open()) {
        cerr << "Ошибка! Не удается открыть файл" << filename << endl;
        return -1;
    }
    while (getline(fin, str)) {
        linecout++; // Увеличиваем счетчик
    }
    fin.close();
    return linecout;
}

struct Condition {
    string column;
    string value;
    string logicalOp; // AND или OR
};

struct BaseDate {
    string nameBD; // База данных самолетов
    int rowLimits; // Установленный лимит
    SinglyLinkedList tablesname; // Список названий таблиц
    Hash_table coloumnHash; // Хэш таблица для работы со столбцами
    Hash_table fileCountHash; // Хэш таблица для количества файлов таблиц

    void parser() {
        nlohmann::json Json;
        ifstream fin("../base_date.json");
        if (!fin.is_open()) {
            cerr << "Ошибка! Не удалось открыть файл Json!" << endl;
            exit(1);
        }
        fin >> Json;

        nameBD = Json["name"].get<string>();
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
                // Добавляем первичный ключ
                columnString = tableName + "_pk," + columnString; 
                if (!columnString.empty()) columnString.pop_back(); // Убираем последний запятую
                coloumnHash.insert(tableName, columnString);
                fileCountHash.insert(tableName, "1");
            }
        } else {
            cerr << "Структура не найдена!" << endl;
            exit(1);
        }
    }

    void createdirect() {
        string baseDir = "../" + nameBD;
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

            // Файл блокировки
            string lockFilePath = tableDir + "/" + tableName + "_lock.txt";
            ofstream lockFile(lockFilePath);
            if (lockFile.is_open()) {
                lockFile << "open";
                lockFile.close();
            }

            // Файл первичного ключа
            string pkFilePath = tableDir + "/" + tableName + "_pk_sequence.txt";
            ofstream pkFile(pkFilePath);
            if (pkFile.is_open()) {
                pkFile << "1"; 
                pkFile.close();
            } else {
                cerr << "Ошибка! Не удалось открыть файл для записи: " << pkFilePath << endl;
            }
        }
    }

    void checkInsert(string& table, string& values) {
        string lockFilePath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        if (fileread(lockFilePath) != "open") {
            cerr << "Ошибка! Таблица " << table << " заблокирована другим пользователем!" << endl;
            return;
        }

        // Увеличиваем первичный ключ
        string pkFilePath = "../" + nameBD + "/" + table + "/" + table + "_pk_sequence.txt";
        string pkValue = fileread(pkFilePath);
        if (pkValue.empty()) {
            cerr << "Ошибка! Не удалось прочитать значение первичного ключа!" << endl;
            return;
        }

        int pkInt = stoi(pkValue); // Текущий первичный ключ (не увеличиваем здесь)
        
        // Меняем на 1, так как записи начинаются с 1
        filerec(pkFilePath, to_string(pkInt + 1));

        // Обработка количества файлов
        string fileCountStr;
        if (!fileCountHash.get(table, fileCountStr)) {
            cerr << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
            return;
        }

        int fileCount = stoi(fileCountStr);
        string csvFilePath = "../" + nameBD + "/" + table + "/" + to_string(fileCount) + ".csv";

        // Проверка количества строк
        int lineCount = countingLine(csvFilePath);
        if (lineCount >= rowLimits) {
            ++fileCount; // Создаем новый файл
            fileCountHash.remove(table);
            fileCountHash.insert(table, to_string(fileCount));
            csvFilePath = "../" + nameBD + "/" + table + "/" + to_string(fileCount) + ".csv";
        }

        ofstream csvFile(csvFilePath, ios::app);
        if (!csvFile.is_open()) {
            cerr << "Ошибка с открытием файла " << csvFilePath << " для записи!" << endl;
            return;
        }

        if (lineCount == 0) {
            string columnString; // Запись заголовков в файл, если он пуст
            if (!coloumnHash.get(table, columnString)) {
                cerr << "Ошибка! Не удалось получить названия столбцов для таблицы " << table << endl;
                return;
            }
            csvFile << columnString << endl;
        }

        // Запись значения с первичным ключом
        csvFile << pkInt << ',' << values << '\n'; 
        csvFile.close();

        cout << "Команда выполнена успешно!" << endl;
    }

    void insert(string& command) {
        string valuesPrefix = "VALUES";
        size_t position = command.find_first_of(' ');

        if (position == string::npos) {
            cout << "Ошибка! Синтаксис команды нарушен!" << endl;
            return;
        }

        string table = command.substr(0, position); // Извлечение названия таблицы
        command.erase(0, position + 1); // Удаление названия таблицы из команды

        // Удаление пробелов до и после названия таблицы
        table.erase(remove_if(table.begin(), table.end(), ::isspace), table.end());

        if (!tablesname.find(table)) {
            cout << "Ошибка! Таблица не найдена!" << endl;
            return;
        }

        if (command.substr(0, valuesPrefix.size()) != valuesPrefix) {
            cout << "Ошибка! Префикс VALUES отсутствует!" << endl;
            return;
        }

        command.erase(0, valuesPrefix.size() + 1); // Удаление префикса VALUES и пробела
        position = command.find_first_of(')'); 

        if (position == string::npos || command.empty() || 
            command.front() != '(' || command.back() != ')') {
            cout << "Ошибка! Синтаксис команды нарушен!" << endl;
            return;
        }

        command.erase(0, 1); // Удаление открывающей скобки
        command.pop_back();   // Удаление закрывающей скобки
        command.erase(remove_if(command.begin(), command.end(), ::isspace), command.end()); // Удаление пробелов

        string columnData; // Получаем названия столбцов и проверяем количество
        if (!coloumnHash.get(table, columnData)) {
            cerr << "Ошибка! Не удалось получить названия столбцов для таблицы " << table << endl;
            return;
        }

        // Подсчет количества столбцов
        int countColumns = count(columnData.begin(), columnData.end(), ',') + 1; // Количество столбцов
        int countValues = count(command.begin(), command.end(), ',') + 1; // Количество значений

        if (countColumns != countValues + 1) { // +1 для первичного ключа
            cout << "Ошибка! Количество значений не совпадает с количеством колонок!" << endl;
            return;
        }

        checkInsert(table, command); // Вызов функции вставки
    }

    void delWhere(string& conditions, string& table) {
        // Разделение условий на подусловия
        Hash_table conditionTable; // Используем хэш-таблицу для условий

        stringstream conditionStream(conditions);
        string condition;
        while (getline(conditionStream, condition, ' ')) {
            if (condition != "AND" && condition != "OR") {
                Condition cond;
                cond.column = condition; // Имя колонки
                conditionStream >> condition; // Получаем оператор
                if (condition != "=") {
                    cerr << "Ошибка! Оператор " << condition << " не поддерживается!" << endl;
                    return;
                }

                conditionStream >> cond.value; // Получаем значение
                // Вставляем условие в хэш-таблицу
                conditionTable.insert(cond.column, cond.value);
            }
            string logicalOp;
            conditionStream >> logicalOp; // Получаем логический оператор (AND, OR)
            if (logicalOp == "AND" || logicalOp == "OR") {
                conditionTable.insert(logicalOp, ""); // Добавление логического оператора
            } else {
                conditionStream.putback(' '); // Возврат символа в поток
            }
        }

        executeDeleteWithConditions(conditionTable, table);
    }

    void executeDeleteWithConditions(Hash_table& conditions, const string& table) {
        string filepath;
        int index = tablesname.getindex(table);
        if (index == -1) {
            cout << "Ошибка, нет такой таблицы!" << endl;
            return;
        }

        // Проверка блокировки
        if (checkLockTable(table)) {
            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "close");

            // Получение индексов столбцов
            Hash_table columnIndexHash;
            for (const string& condCol : { /* ваши условия для колонок */ }) { // Массив колонок условий
                string columns;
                if (!coloumnHash.get(condCol, columns)) {
                    cerr << "Ошибка! Не удалось получить названия столбцов для таблицы " << table << endl;
                    return;
                }

                stringstream ss(columns);
                string col;
                int colIndex = 0;
                while (getline(ss, col, ',')) {
                    if (col == condCol) {
                        columnIndexHash.insert(condCol, to_string(colIndex));
                        break;
                    }
                    colIndex++;
                }
            }

            // Удаление строк из файлов
            int fileCount = stoi(fileCountHash.get(table));
            for (int fileIndex = 1; fileIndex <= fileCount; ++fileIndex) {
                filepath = "../" + nameBD + "/" + table + "/" + to_string(fileIndex) + ".csv";
                string csvData = fileread(filepath);
                stringstream csvStream(csvData);
                string newCsvData;
                string row;

                while (getline(csvStream, row)) {
                    bool shouldRemove = checkRowAgainstConditions(row, columnIndexHash, conditions);
                    if (!shouldRemove) {
                        newCsvData += row + "\n";
                    }
                }

                filerec(filepath, newCsvData);
            }

            filepath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else {
            cout << "Ошибка, таблица используется другим пользователем!" << endl;
        }
    }

    bool checkRowAgainstConditions(const string& row, Hash_table& columnIndexes, Hash_table& conditions) {
        stringstream ss(row);
        string token;
        string column;
        bool result = true; // Базовое значение для AND

        while (getline(ss, token, ',')) {
            // Проверяем соответствие условий с соответствующими значениями
            if (columnIndexes.get(column, token)) { // Если индекс колонки существует
                string expectedValue;
                conditions.get(column, expectedValue);
                result = (token == expectedValue); // Проверка равенства значения
                if (!result) break; // Прекращаем проверку при несоответствии
            }
        }

        return result; // Вернуть результат проверки
    }

    void commands(string& command) {
        if (command.substr(0, 11) == "INSERT INTO") {
            command.erase(0, 12);
            insert(command);
        } else if (command.substr(0, 11) == "DELETE FROM") {
            command.erase(0, 12);
            size_t wherePos = command.find("WHERE");
            if (wherePos != string::npos) {
                string table = command.substr(0, wherePos); // Название таблицы
                command.erase(0, wherePos + 6); // Удаление 'WHERE '
                delWhere(command, table);
            } else {
                cout << "Ошибка! Отсутствует условие WHERE!" << endl;
            }
        } else if (command == "STOP") {
            exit(0);
        } else {
            cout << "Ошибка! Введенной команды нет!" << endl; 
        }
    }
};

int main() {

    BaseDate airport;

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