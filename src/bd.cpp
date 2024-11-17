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
    fout << data;
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

    void Insert(string& command) { // Функция insert
    size_t position = command.find_first_of(' ');
        if (position == string::npos) {
            cerr << "Ошибка, нарушен синтаксис команды!" << endl;
            return;
        }

        string table = command.substr(0, position);
        bool tableExists = false; // Проверка существования таблицы
        for (size_t i = 0; i < tablesname.elementCount; ++i) {
            if (tablesname.getElementAt(i) == table) {
                tableExists = true;
                break;
            }
        }

        if (!tableExists) {
            cerr << "Ошибка, нет такой таблицы!" << endl;
            return;
        }   

        string valuesPart = command.substr(position + 1);
        if (valuesPart.substr(0, 7) != "values ") {
            cerr << "Ошибка, нарушен синтаксис команды!" << endl;
            return;
        }
        valuesPart.erase(0, 7);

        if ((valuesPart.front() != '(') || (valuesPart.back() != ')')) {
            cerr << "Ошибка, нарушен синтаксис команды!" << endl;
            return;
        }

        valuesPart.erase(0, 1); // Удаляем открывающую скобку
        valuesPart.pop_back(); // Удаляем закрывающую скобку
        valuesPart.erase(remove_if(valuesPart.begin(), valuesPart.end(), ::isspace), valuesPart.end());

        string pkFilePath = "../" + nameBD + "/" + table + "/" + table + "_pk_sequence.txt";
        string pkValue = fileread(pkFilePath);
        int pkInt = stoi(pkValue) + 1;
        filerec(pkFilePath, to_string(pkInt));

        string lockFilePath = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        string lockStatus = fileread(lockFilePath);
        if (lockStatus != "open") {
            cerr << "Ошибка, не удалось открыть таблицу!" << endl;
            return;
        }

        string fileCountStr;
        if (!fileCountHash.get(table, fileCountStr)) {
            cerr << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
            return;
        }

        int fileCount = stoi(fileCountStr);
        string csvFilePath = "../" + nameBD + "/" + table + "/" + fileCountStr + ".csv";
        int lineCount = countingLine(csvFilePath);

        if (lineCount >= rowLimits) {
            ++fileCount;
            fileCountHash.remove(table); // Обновление количества файлов
            fileCountHash.insert(table, to_string(fileCount));
            csvFilePath = "../" + nameBD + "/" + table + "/" + to_string(fileCount) + ".csv";
        }

        ofstream csvFile(csvFilePath, ios::app);
        if (!csvFile.is_open()) {
            cerr << "Ошибка: Не удалось открыть файл " << csvFilePath << " для записи." << endl;
            return;
        }

        if (lineCount == 0) {
            string columnString;
            if (!coloumnHash.get(table, columnString)) {
                cerr << "Ошибка: Не удалось получить названия столбцов для таблицы " << table << endl;
                return;
            }
            csvFile << columnString << endl;
        }
        csvFile << pkInt << ',' << valuesPart << '\n'; // Запись данных в файл
        csvFile.close();
        cout << "Команда выполнилась успешно!" << endl;
    }
};