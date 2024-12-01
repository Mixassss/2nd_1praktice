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
                string columnString = tableName + "_pk,"; // Добавляем первичный ключ
                for (auto& column : item.value().items()) {
                    columnString += column.value().get<string>() + ",";
                }
                if (!columnString.empty()) columnString.pop_back();
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

        int pkInt = stoi(pkValue) + 1;
        filerec(pkFilePath, to_string(pkInt));

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

    void delAll(string& table) { // Удаление всех строк из таблицы, кроме заголовков
        string lockFilePath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        filerec(lockFilePath, "close"); // Блокируем таблицу

        // Получаем количество файлов для таблицы
        string fileCountStr;
        if (!fileCountHash.get(table, fileCountStr)) {
            cerr << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
            return;
        }

        int fileCount = stoi(fileCountStr);
        for (int i = 1; i <= fileCount; ++i) {
            string csvFilePath = "../" + nameBD + "/" + table + "/" + to_string(i) + ".csv";
            
            // Открываем файл для чтения
            ifstream inFile(csvFilePath);
            ofstream outFile(csvFilePath + ".tmp"); // Временный файл для записи

            if (!inFile.is_open() || !outFile.is_open()) {
                cerr << "Ошибка: Не удалось открыть файл " << csvFilePath << endl;
                continue;
            }

            string line;
            int lineNumber = 0;

            // Считываем строки и записываем только заголовок
            while (getline(inFile, line)) {
                if (lineNumber == 0) {
                    outFile << line << endl; // Записываем заголовок
                }
                lineNumber++;
            }

            inFile.close();
            outFile.close();

            // Заменяем оригинальный файл временным
            rename((csvFilePath + ".tmp").c_str(), csvFilePath.c_str());
        }

        // Обновление первичного ключа
        string pkFilePath = "../" + nameBD + "/" + table + "/" + table + "_pk_sequence.txt";
        filerec(pkFilePath, "1"); // Сбрасываем первичный ключ на 1

        filerec(lockFilePath, "open"); // Разблокируем таблицу
        cout << "Все строки таблицы удалены, первичный ключ обновлён!" << endl;
    }

    void delWhere(string& conditions, string& table) {
        if (conditions.empty()) {
            cout << "Ошибка! Неправильное условие!" << endl;
            return;
        }

        size_t wherePos = conditions.find("WHERE ");
        if (wherePos == string::npos) {
            cout << "Ошибка! Отсутствует условие WHERE!" << endl;
            return;
        }

        string condition = conditions.substr(wherePos + 6); // Извлекаем условие после 'WHERE '

        // Убираем пробелы
        condition.erase(remove_if(condition.begin(), condition.end(), ::isspace), condition.end());

        string logicalOp = ""; // Определение логического оператора
        size_t logicOpPos = condition.find("AND");
        if (logicOpPos == string::npos) {
            logicOpPos = condition.find("OR");
            if (logicOpPos != string::npos) logicalOp = "OR";
        } else {
            logicalOp = "AND";
        }

        string column, value; // Разделяем колонку и значение
        size_t eqPos = condition.find('=');
        if (eqPos == string::npos) {
            cout << "Ошибка! Неправильное условие!" << endl;
            return;
        }
        column = condition.substr(0, eqPos);
        value = condition.substr(eqPos + 1);
        
        // Удаляем одинарные кавычки из значения
        if (!value.empty() && value.front() == '\'' && value.back() == '\'') {
            value = value.substr(1, value.size() - 2);
        }

        string columnData; // Удаление строк
        if (!coloumnHash.get(table, columnData)) {
            cout << "Ошибка! Нет таких столбцов в таблице!" << endl;
            return;
        }

        deleteRows(table, column, value, logicalOp);
    }

    void deleteRows(string& table, string& column, string& value, const string& logicalOp = "") {
        // Блокировка таблицы
        string lockFilePath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        filerec(lockFilePath, "close");

        // Проверка наличия столбца
        string columnData;
        if (!coloumnHash.get(table, columnData)) {
            cout << "Ошибка! Нет таких столбцов в таблице!" << endl;
            filerec(lockFilePath, "open");
            return;
        }

        // Получение индекса столбца
        int columnIndex = -1;
        stringstream ss(columnData);
        string colName;
        int currentIndex = 0;

        while (getline(ss, colName, ',')) {
            if (colName == column) {
                columnIndex = currentIndex;
                break;
            }
            currentIndex++;
        }

        if (columnIndex == -1) {
            cout << "Ошибка! Нет такого столбца!" << endl;
            filerec(lockFilePath, "open");
            return;
        }

        // Чтение количества файлов
        string fileCountStr;
        if (!fileCountHash.get(table, fileCountStr)) {
            cerr << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
            filerec(lockFilePath, "open");
            return;
        }

        int fileCount = stoi(fileCountStr);
        for (int i = 1; i <= fileCount; ++i) {
            string csvFilePath = "../" + nameBD + "/" + table + "/" + to_string(i) + ".csv";
            string csvContent = fileread(csvFilePath);
            stringstream csvStream(csvContent);
            string newCsvContent;
            string line;

            while (getline(csvStream, line)) {
                stringstream lineStream(line);
                string token;
                int currentColumnIndex = 0;
                bool shouldRemove = false;

                while (getline(lineStream, token, ',')) {
                    bool matchFound = (currentColumnIndex == columnIndex && token == value);
                    if ((logicalOp == "AND" && matchFound) || (logicalOp == "OR" && !matchFound)) {
                        shouldRemove = (logicalOp == "OR") ? false : true;
                        break;
                    }
                    currentColumnIndex++;
                }

                // Сохраняем строку, если она не подлежит удалению
                if (!shouldRemove) {
                    newCsvContent += line + "\n";
                }
            }

            filerec(csvFilePath, newCsvContent); // Обновляем файл CSV
        }

        filerec(lockFilePath, "open"); // Разблокируем таблицу
        cout << "Команда выполнена успешно!" << endl;
    }

    void commands(string& command) { // Функция для поддержки команд
        if (command.substr(0, 11) == "INSERT INTO") {
            command.erase(0, 12);
            insert(command);
        } else if (command.substr(0, 11) == "DELETE FROM") {
            command.erase(0, 12); // Удаление "DELETE FROM"
            size_t wherePos = command.find("WHERE");
            string table, conditions;

            if (wherePos != string::npos) {
                table = command.substr(0, wherePos); // Имя таблицы
                conditions = command.substr(wherePos); // Условия
            } else {
            table = command; // Если нет условий, то полное имя таблицы
            conditions = ""; // Условия пустое
            }
        
            if (conditions.empty()) {
                delAll(table); // Если нет условий WHERE, то удаляем все строки
                } else {
                delWhere(conditions, table); // Удаление с условиями
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