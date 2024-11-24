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

    void parser() { // Парсинг json
        nlohmann::json Json;
        ifstream fin("../base_date.json");
        if (!fin.is_open()) {
            cerr << "Ошибка, не удалось открыть файл Json!" << endl;
            exit(1);
        }
        fin >> Json;
        fin.close();
        
        if (!Json["name"].is_string()) {
            cerr << "Неверный формат JSON или каталог не найден!" << endl;
            exit(1);
        } 
        nameBD = Json["name"]; // Парсим название БД

        if (Json["tuples limit"].is_number_integer()) { // Извлечение лимита
            rowLimits = Json["tuples limit"].get<int>();
        } else {
            cerr << "Неверный формат лимита!" << endl;
            exit(1);
        }
        
        if (Json.contains("structure") && Json["structure"].is_object()) {
            for (auto& item : Json["structure"].items()) {
                string tableName = item.key();
                tablesname.pushBack(tableName); // Добавляем название таблицы

                string stolbName = item.key() + "_pk_sequence,"; // Добавление столбцов в хеш таблицу
                for (auto& colona : item.value().items()) {
                    stolbName += colona.value().get<string>() + ",";
                }
                if (!stolbName.empty()) stolbName.pop_back();
                coloumnHash.insert(tableName, stolbName); // Вставка столбцов
                fileCountHash.insert(tableName, "1"); // Инициализация количества файлов
            }
        } else {
            cerr << "Структура не найдена!" << endl;
            exit(1);
        }
    }

    void createdirect() { // Функция для создания рабочей директории
        string baseDir = "../" + nameBD;
        filesystem::create_directories(baseDir);

        for (int i = 0; i < tablesname.elementCount; ++i) {
            string tableName = tablesname.getElementAt(i);
            string tableDir = baseDir + "/" + tableName;

            filesystem::create_directories(tableDir);

            // Получаем названия столбцов в строку
            string coloumnString;
            if (!coloumnHash.get(tableName, coloumnString)) {
                cerr << "Не удалось получить столбцы для таблицы: " << tableName << endl;
                continue; // Пропускаем создание директории, если данные не найдены
            }

            string csvFile = tableDir + "/1.csv";
            ofstream csv(csvFile);
            if (csv.is_open()) {
                csv << coloumnString << endl; // Запись в файл
                csv.close();
            }

            // Блокировка таблицы
            string lockFilePath = tableDir + "/" + tableName + "/" + tableName + "_lock.txt";
            ofstream lockFile(lockFilePath);
            if (lockFile.is_open()) {
                lockFile << "open"; // Текущий статус блокировки
                lockFile.close();
            }

            // Начальное значение первичного ключа
            string pkFilePath = tableDir + "/" +  tableName + "/" + tableName + "_pk_sequence.txt";
            ofstream pkFile(pkFilePath);
            if (pkFile.is_open()) {
                pkFile << "1"; // Начальное значение
                pkFile.close();
            } else {
                cerr << "Ошибка! Не удалось открыть файл " << pkFilePath << " для записи." << endl;
            }
        }
    }

    void insert(string& command) { // Функция вставки
        string valuesPrefix = "values "; // Определение префикса values
        string table;
        size_t position = command.find_first_of(' ');

        if (position == string::npos) { // Проверка на наличие пробела
            cout << "Ошибка! Синтаксис команды нарушен!" << endl;
            return;
        }

        table = command.substr(0, position); // Извлечение названия таблицы
        command.erase(0, position + 1); // Удаление названия таблицы из команды

        if (tablesname.getHead() == nullptr || !tablesname.find(table)) { // Проверка на существование таблицы
            cout << "Ошибка! Синтаксис команды нарушен!" << endl;
            return;
        }

        if (command.substr(0, valuesPrefix.size()) != valuesPrefix) { // Проверка на наличие префикса values
            cout << "Ошибка! Синтаксис команды нарушен!" << endl;
            return;
        }

        command.erase(0, valuesPrefix.size()); // Удаление префикса values
        position = command.find_first_of(' ');

        if (position != string::npos || command.empty() || // Проверка на синтаксис
            command.front() != '(' || command.back() != ')') {
            cout << "Ошибка! Синтаксис команды нарушен!" << endl;
            return;
        }

        command.erase(0, 1); // Удаление открывающей скобки
        command.pop_back();   // Удаление закрывающей скобки
        command.erase(remove_if(command.begin(), command.end(), ::isspace), command.end()); // Удаление пробелов

        // Проверка на количество колонок
        string columnData;
        if (!coloumnHash.get(table, columnData)) {
            cerr << "Ошибка! Не удалось получить названия столбцов для таблицы " << table << endl;
            return;
        }

        // Считаем количество колонок
        int countColumns = std::count(columnData.begin(), columnData.end(), ',') + 1;
        int countValues = std::count(command.begin(), command.end(), ',') + 1;

        if (countColumns != countValues) {
            cout << "Ошибка! Количество значений не совпадает с количеством колонок!" << endl;
            return;
        }

        checkInsert(table, command); // Вызов функции вставки
    }

    void checkInsert(string& table, string& values) {
        string pkFilePath = "../" + nameBD + "/" + table + "/" + table + "_pk_sequence.txt";
        string pkValue = fileread(pkFilePath);

        if (pkValue.empty()) { // Проверка успешности чтения значения первичного ключа
            cerr << "Ошибка! Не удалось прочитать значение первичного ключа!" << endl;
            return;
        }

        int pkInt = stoi(pkValue) + 1;
        filerec(pkFilePath, to_string(pkInt));

        string lockFilePath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        string lockStatus = fileread(lockFilePath);
        if (lockStatus != "open") {
            cerr << "Ошибка! Таблица не открыта!" << endl;
            return;
        }

        string fileCountStr;
        if (!fileCountHash.get(table, fileCountStr)) {
            cerr << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
            return;
        }

        int fileCount = stoi(fileCountStr);
        string csvFilePath = "../" + nameBD + "/" + table + "/" + to_string(fileCount) + ".csv";
        int lineCount = countingLine(csvFilePath);

        if (lineCount >= rowLimits) {
            ++fileCount;
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
            string columnString;
            if (!coloumnHash.get(table, columnString)) {
                cerr << "Ошибка! Не удалось получить названия столбцов для таблицы " << table << endl;
                return;
            }
            csvFile << columnString << endl;
        }
        csvFile << pkInt << ',' << values << '\n';
        csvFile.close();
        cout << "Команда выполнена успешно!" << endl;
    }

    void delet(string& command) { // Удаление данных
        string table, conditions;
        size_t pos = command.find_first_of(' ');

        if (pos != string::npos) {
            table = command.substr(0, pos);
            conditions = command.substr(pos + 1);
        } else {
            table = command;
        }

        if (!tablesname.find(table)) { // Проверка на существование таблицы
            cout << "Ошибка! Нет такой таблицы." << endl;
            return;
        }

        if (conditions.empty()) {
            delAll(table);
        } else if (conditions.substr(0, 6) == "WHERE ") {
            conditions.erase(0, 6);
            delWhereProcess(conditions, table);
        } else {
            cout << "Ошибка! Ожидался 'WHERE'." << endl;
        }
    }

    void delAll(string& table) { // Удаление всех строк из таблицы
        string lockFilePath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        filerec(lockFilePath, "close"); // Блокируем таблицу

        string fileCountStr;
        if (!fileCountHash.get(table, fileCountStr)) {
            cerr << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
            return;
        }

        int fileCount = stoi(fileCountStr);
        for (int i = 1; i <= fileCount; ++i) {
            string csvFilePath = "../" + nameBD + "/" + table + "/" + to_string(i) + ".csv";
            filerec(csvFilePath, ""); // Очищаем файл
        }

        filerec(lockFilePath, "open"); // Разблокируем таблицу
        cout << "Все строки таблицы удалены!" << endl;
    }

    void delWhereProcess(string& conditions, string& table) { // Удаление строк по условию
        if (conditions.empty()) {
            cout << "Ошибка! Неправильное условие!" << endl;
            return;
        }

        ssize_t pos = conditions.find(" ");
        
        if (pos == string::npos) {
            cout << "Ошибка! Неправильное условие!" << endl;
            return;
        }

        string column, value;
        string logicOp = (conditions.find("OR") != string::npos) ? "OR" : "AND";

        // Извлечение имени столбца и значения
        conditions.insert(pos, " = "); // вставка оператора равенства
        column = conditions.substr(0, pos);
        value = conditions.substr(pos + 3); // Убираем пробелы и '='
        value.erase(remove_if(value.begin(), value.end(), ::isspace), value.end());
            
        // Убираем "WHERE" и пробелы
        column.erase(remove_if(column.begin(), column.end(), ::isspace), column.end());
        
        // Проверяем наличие колонки с использованием точечной нотации
        if (column.find('.') == string::npos) {
            cout << "Ошибка! Используйте точечную нотацию для указания столбца." << endl;
            return;
        }

        string tableName = column.substr(0, column.find('.'));
        string columnName = column.substr(column.find('.') + 1);

        if (!tablesname.find(tableName)) {
            cout << "Ошибка! Нет такой таблицы!" << endl;
            return;
        }

        string columnData;
        if (!coloumnHash.get(tableName, columnData)) {
            cout << "Ошибка! Нет таких столбцов в таблице!" << endl;
            return;
        }

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
            return;
        }

        string lockFilePath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        filerec(lockFilePath, "close");

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
                    if (currentColumnIndex == columnIndex && token == value) { // Проверка условия удаления
                        shouldRemove = true;
                        break;
                    }
                    currentColumnIndex++;
                }

                if ((logicOp == "AND" && shouldRemove) || (logicOp == "OR" && !shouldRemove)) {
                    continue; // Условие сработало, пропускаем строку
                } else {
                    newCsvContent += line + "\n"; // сохраняем строку
                }
            }
            filerec(csvFilePath, newCsvContent); // Обновляем файл CSV
        }

        filerec(lockFilePath, "open"); // Разблокируем таблицу
        cout << "Команда выполнена успешно!" << endl;
    }
};