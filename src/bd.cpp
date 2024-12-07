#include "../include/json.hpp"
#include "../include/list.h"
#include "../include/ht.h"

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

struct BaseData {
    string nameBD; // Название БД
    int rowLimits; // Лимит строк
    SinglyLinkedList<string> tablesname; // Список названий таблиц
    Hash_table<string, string> coloumnHash; // Хэш таблица для работы со столбцами
    Hash_table<string, int> fileCountHash; // Хэш таблица для количества файлов таблиц

    struct Filters { // Структура для фильтрации
        string colona; // Имя столбца
        string value; // Значение для сравнения
        string logicOP; // Логический оператор (AND/OR)
    };

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
                fileCountHash.insert(tableName, 1);
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

            createLockFile(tableDir, tableName);
            createPKFile(tableDir, tableName);
        }
    }

    void createLockFile(const string& dir, const string& tableName) {
        ofstream lockFile(dir + "/" + tableName + "_lock.txt");
        lockFile << "open";
    }

    void createPKFile(const string& dir, const string& tableName) {
        ofstream pkFile(dir + "/" + tableName + "_pk_sequence.txt");
        if (pkFile.is_open()) {
            pkFile << "1"; 
            pkFile.close();
        } else {
            cerr << "Ошибка! Не удалось открыть файл для записи! " << endl;
        }
    }

    /// Функции для DELETE FROM ///
    void Delete(string& command) { // ф-ия обработки команды DELETE
        string table, conditions;
        int position = command.find_first_of(' ');
        if (position != -1) {
            table = command.substr(0, position);
            conditions = command.substr(position + 1);
        } else table = command;
        // Проверка существования таблицы в хэш-таблице
        int fileCount; // Переменная для хранения количества файлов
        if (fileCountHash.get(table, fileCount)) { // Проверка наличия таблицы
            if (conditions.empty()) { // Если нет условий, удаляем все
                delAll(table);
                } else {
                    if (conditions.substr(0, 6) == "WHERE ") { // проверка наличия where
                        conditions.erase(0, 6);
                        Hash_table<string, Filters> yslov; // Используем хэш-таблицу для условий
                        Filters filter;
                        position = conditions.find_first_of(' ');
                        if (position != -1) { // проверка синтаксиса
                            filter.colona = conditions.substr(0, position);
                            conditions.erase(0, position + 1);
                            // Проверка наличия столбца в хэш-таблице
                            string columnString;
                            if (coloumnHash.get(table, columnString)) {
                                stringstream ss(columnString);
                                bool check = false;
                                string str;
                                while (getline(ss, str, ',')) {
                                    if (str == filter.colona) {
                                    check = true;
                                    break;
                                }
                            }
                            if (check) { // проверка столбца
                                if (conditions[0] == '=' && conditions[1] == ' ') { // проверка синтаксиса
                                    conditions.erase(0, 2);
                                    position = conditions.find_first_of(' ');
                                    if (position == -1) { // если нет лог. оператора
                                        filter.value = conditions;
                                        deleteValue(table, filter.colona, filter.value);
                                    } else { // если есть логический оператор
                                        filter.value = conditions.substr(0, position);
                                        conditions.erase(0, position+1);
                                        yslov.insert(filter.colona, filter); // Добавляем в хэш-таблицу
                                        position = conditions.find_first_of(' ');
                                        if ((position != -1) && (conditions.substr(0, 2) == "OR" || conditions.substr(0, 3) == "AND")) {
                                            filter.logicOP = conditions.substr(0, position);
                                            conditions.erase(0, position + 1);
                                            position = conditions.find_first_of(' ');
                                            if (position != -1) {
                                                filter.colona = conditions.substr(0, position);
                                                conditions.erase(0, position+1);
                                                // Проверка наличия столбца во втором условии
                                                if (coloumnHash.get(table, columnString)) {
                                                    stringstream iss(columnString);
                                                    bool check = false;
                                                    while (getline(iss, str, ',')) {
                                                        if (str == filter.colona) {
                                                            check = true;
                                                            break;
                                                        }
                                                    }
                                                    if (check) { // проверка столбца
                                                        if (conditions[0] == '=' && conditions[1] == ' ') { // проверка синтаксиса
                                                            conditions.erase(0, 2);
                                                            position = conditions.find_first_of(' ');
                                                            if (position == -1) {
                                                                filter.value = conditions;
                                                                yslov.insert(filter.colona, filter); // Добавляем во второе условие
                                                                deleteFilter(yslov, table);
                                                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                                    } else cout << "Ошибка, нет такого столбца!" << endl;
                                                } else cout << "Ошибка, нет такой таблицы!" << endl;
                                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                    }
                                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                            } else cout << "Ошибка, нет такого столбца!" << endl;
                        } else cout << "Ошибка, нет такой таблицы!" << endl;
                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
            }
        } else cout << "Ошибка, нет такой таблицы!" << endl; 
    }

    void delAll(string& table) { // ф-ия удаления всех строк таблицы
        string fin;
        int index = tablesname.getIndex(table); // Получить индекс таблицы из списка
        string columnString; // Строка для хранения заголовков столбцов

        if (checkLockTable(table)) {
            lockTable(table, false); // Закрытие таблицы для работы
            int fileCount;
            if (!fileCountHash.get(table, fileCount)) {
                cerr << "Не удалось получить количество файлов для таблицы: " << table << endl;
                return;
            }

            // Получаем заголовки из 1.csv
            fin = "../" + nameBD + "/" + table + "/1.csv";
            ifstream headerFile(fin);
            if (headerFile.is_open()) {
                getline(headerFile, columnString); // Читаем заголовки
                headerFile.close();
                } else {
                    cerr << "Не удалось открыть файл для чтения заголовков!" << endl;
                    return;
                }

                // Цикл для очистки всех файлов таблицы, кроме 1.csv
                for (int all = 1; all <= fileCount; ++all) {
                    fin = "../" + nameBD + "/" + table + "/" + to_string(all) + ".csv";
                    if (all == 1) {
                        filerec(fin, columnString + "\n");
                    } else {
                        filerec(fin, ""); // Очищаем содержимое файла
                    }
                }

                lockTable(table, true); // Снова открываем таблицу
                cout << "Команда выполнена успешно!" << endl;
            } else {
            cout << "Ошибка! Таблица используется другим пользователем!" << endl;
        }
    }

    void deleteValue(string& table, string& stolb, string& values) { // Функция удаления по значению
        string fin;
        int index = tablesname.getIndex(table);
        if (checkLockTable(table)) {
            lockTable(table, false); // Закрытие таблицы для работы

            string colonaValues;
            if (!coloumnHash.get(table, colonaValues)) {
                cerr << "Не удалось получить значения столбца для таблицы: " << table << endl;
                lockTable(table, true); // Открываем таблицу
                return;
            }

            // Извлекаем индекс столбца
            stringstream ss(colonaValues);
            string columnValue;
            int colonaIndex = -1;
            int thisIndex = 0;

            while (getline(ss, columnValue, ',')) {
                if (columnValue == stolb) {
                    colonaIndex = thisIndex;
                    break;
                }
                thisIndex++;
            }

            if (colonaIndex == -1) {
                cerr << "Столбец не найден!" << endl;
                lockTable(table, true); // Открываем таблицу
                return;
            }

            int fileCount;
            if (!fileCountHash.get(table, fileCount)) {
                cerr << "Не удалось получить количество файлов для таблицы: " << table << endl;
                lockTable(table, true); // Открываем таблицу
                return;
            }

            int copyCount = fileCount; // Получаем количество файлов таблицы
            bool valueFound = false; // Флаг для отслеживания, найдено ли значение
            string filteredLines; // Строка для хранения обновленных данных
            while (copyCount > 0) {
                fin = "../" + nameBD + "/" + table + "/" + to_string(copyCount) + ".csv";
                string text = fileread(fin);
                stringstream lineStream(text);
                string filteredLines;
                string textLine;

                while (getline(lineStream, textLine)) {
                    stringstream tokenStream(textLine);
                    string token;
                    bool shouldRemove = false;
                    int thisColumnIndex = 0;
                    string updatedLine;

                    while (getline(tokenStream, token, ',')) {
                        if (thisColumnIndex == colonaIndex) {
                            if (token == values) {
                                valueFound = true; // Значение найдено
                                token = " "; // Устанавливаем значение в пустую строку
                            }
                        }
                        updatedLine += token; // Добавляем текущий токен
                        if (tokenStream.peek() != EOF) { // Проверяем, если это не последний токен
                            updatedLine += ",";
                        }
                        thisColumnIndex++;
                    }
                    updatedLine += "\n"; // Добавляем новую строку
                    filteredLines += updatedLine; // Добавляем обновленную строку в общий результат
                }
            filerec(fin, filteredLines);
            copyCount--;
        }
        if (!valueFound) {
            cerr << "Ошибка: значение '" << values << "' не найдено в столбце '" << stolb << "'." << endl;
        } else cout << "Команда выполнена успешно!" << endl;

        lockTable(table, true); // Снова открываем таблицу
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }

    void deleteFilter(Hash_table<string, Filters>& filters, string& table) { // Функция удаления по условиям AND и OR
        string fin;
        int index = tablesname.getIndex(table);
        if (checkLockTable(table)) {
            lockTable(table, false); // Закрытие таблицы для работы

            // нахождение индекса столбцов в файле
            SinglyLinkedList<int> colonaIndexes;
            string columnValue;

            // Получаем все условия из хеш-таблицы
            for (int i = 0; i < filters.size(); ++i) {
                Filters filterCondition;
                if (filters.get(to_string(i), filterCondition)) {
                    if (coloumnHash.get(table, columnValue)) {
                        stringstream ss(columnValue);
                        string column;
                        int idx = 0;

                        while (getline(ss, column, ',')) {
                            if (column == filterCondition.colona) {
                                colonaIndexes.pushBack(idx);
                                break;
                            }
                            idx++;
                        }   
                    } else {
                        cerr << "Столбцы не найдены для таблицы: " << table << endl;
                        return;
                    }
                }
            }
            
            // Удаление строк
            int copyCount;
            if (!fileCountHash.get(table, copyCount)) {
                cerr << "Не удалось получить количество файлов для таблицы: " << table << endl;
                return;
            }

            while (copyCount > 0) {
                fin = "../" + nameBD + "/" + table + "/" + to_string(copyCount) + ".csv";
                string text = fileread(fin);
                stringstream rowString(text);
                string filteredRows;
                string row;

                while (getline(rowString, row)) {
                bool shouldRemove = false;

                // Проверяем все условия
                for (int i = 0; i < colonaIndexes.size(); ++i) {
                    stringstream iss(row);
                    string token;
                    int currentIndex = 0;
                    bool match = false;

                    while (getline(iss, token, ',')) {
                        if (currentIndex == colonaIndexes.getElementAt(i)) {
                            Filters filterCondition;
                            if (filters.get(to_string(i), filterCondition)) {
                                if (token == filterCondition.value) {
                                    match = true; // Найдено совпадение
                                    break; // Выходим из цикла
                                }
                            }
                        }
                        currentIndex++;
                    }

                    // Если не совпадает с условием, помечаем для удаления
                    if (!match) {
                        shouldRemove = true;
                        break; // Удаляем строку, если не все условия выполнены
                    }
                }

                // Если строка не должна быть удалена, добавляем ее к результату
                if (!shouldRemove) {
                    filteredRows += row + "\n";
                }
            }
            filerec(fin, filteredRows);
            copyCount--;
        }
            lockTable(table, true); // Снова открываем таблицу
            cout << "Команда выполнена успешно!" << endl;
        } else {
            cout << "Ошибка, таблица используется другим пользователем!" << endl;
        }
    }

    /// Функции для инсерта ///
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
        filerec(pkFilePath, to_string(pkInt + 1));

        // Обработка количества файлов
        int fileCount;
        if (!fileCountHash.get(table, fileCount)) {
            cerr << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
            return;
        }

        string csvFilePath = "../" + nameBD + "/" + table + "/" + to_string(fileCount) + ".csv";

        // Проверка количества строк
        int lineCount = countingLine(csvFilePath);
        if (lineCount >= rowLimits) {
            ++fileCount; // Создаем новый файл
            fileCountHash.remove(table);
            fileCountHash.insert(table, fileCount);
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

    void Insert(string& command) {
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


    // ф-ии селекта
    // void isValidSelect(string& command) { // ф-ия проверки ввода команды select
    //     Where conditions;
    //     SinglyLinkedList<Where> cond;

    //     if (command.find_first_of("from") != -1) {
    //         // работа со столбцами
    //         while (command.substr(0, 4) != "from") {
    //             string token = command.substr(0, command.find_first_of(' '));
    //             if (token.find_first_of(',') != -1) token.pop_back(); // удаляем запятую
    //             command.erase(0, command.find_first_of(' ') + 1);
    //             if (token.find_first_of('.') != -1) token.replace(token.find_first_of('.'), 1, " ");
    //             else {
    //                 cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //                 return;
    //             }
    //             stringstream ss(token);
    //             ss >> conditions.table >> conditions.column;
    //             bool check = false;
    //             int i;
    //             for (i = 0; i < nametables.size; ++i) { // проверка, сущ. ли такая таблица
    //                 if (conditions.table == nametables.getvalue(i)) {
    //                     check = true;
    //                     break;
    //                 }
    //             }
    //             if (!check) {
    //                 cout << "Нет такой таблицы!" << endl;
    //                 return;
    //             }
    //             check = false;
    //             stringstream iss(stlb.getvalue(i));
    //             while (getline(iss, token, ',')) { // проверка, сущ. ли такой столбец
    //                 if (token == conditions.column) {
    //                     check = true;
    //                     break;
    //                 }
    //             }
    //             if (!check) {
    //                 cout << "Нет такого столбца" << endl;
    //                 return;
    //             }
    //             cond.push_back(conditions);
    //         }

    //         command.erase(0, command.find_first_of(' ') + 1); // скип from

    //         // работа с таблицами
    //         int iter = 0;
    //         while (!command.empty()) { // пока строка не пуста
    //             string token = command.substr(0, command.find_first_of(' '));
    //             if (token.find_first_of(',') != -1) {
    //                 token.pop_back();
    //             }
    //             int position = command.find_first_of(' ');
    //             if (position != -1) command.erase(0, position + 1);
    //             else command.erase(0);
    //             if (iter + 1 > cond.size || token != cond.getvalue(iter).table) {
    //                 cout << "Ошибка, указаные таблицы не совпадают или их больше!" << endl;
    //                 return;
    //             }
    //             if (command.substr(0, 5) == "where") break; // также заканчиваем цикл если встретился WHERE
    //             iter++;
    //         }
    //         if (command.empty()) {
    //             select(cond);
    //         } else {
    //             if (command.find_first_of(' ') != -1) {
    //                 command.erase(0, 6);
    //                 int position = command.find_first_of(' ');
    //                 if (position != -1) {
    //                     string token = command.substr(0, position);
    //                     command.erase(0, position + 1);
    //                     if (token.find_first_of('.') != -1) {
    //                         token.replace(token.find_first_of('.'), 1, " ");
    //                         stringstream ss(token);
    //                         string table, column;
    //                         ss >> table >> column;
    //                         if (table == cond.getvalue(0).table) { // проверка таблицы в where
    //                             position = command.find_first_of(' ');
    //                             if ((position != -1) && (command[0] == '=')) {
    //                                 command.erase(0, position + 1);
    //                                 position = command.find_first_of(' ');
    //                                 if (position == -1) { // если нет лог. операторов
    //                                     if (command.find_first_of('.') == -1) { // если просто значение
    //                                         conditions.value = command;
    //                                         conditions.check = true;
    //                                         selectWithValue(cond, table, column, conditions);
    //                                     } else { // если столбец
    //                                         command.replace(command.find_first_of('.'), 1, " ");
    //                                         stringstream iss(command);
    //                                         iss >> conditions.table >> conditions.column;
    //                                         conditions.check = false;
    //                                         selectWithValue(cond, table, column, conditions);
    //                                     }

    //                                 } else { // если есть лог. операторы
    //                                     SinglyLinkedList<Where> values;
    //                                     token = command.substr(0, position);
    //                                     command.erase(0, position + 1);
    //                                     if (token.find_first_of('.') == -1) { // если просто значение
    //                                         conditions.value = token;
    //                                         conditions.check = true;
    //                                         values.push_back(conditions);
    //                                     } else { // если столбец
    //                                         token.replace(token.find_first_of('.'), 1, " ");
    //                                         stringstream stream(token);
    //                                         stream >> conditions.table >> conditions.column;
    //                                         conditions.check = false;
    //                                         values.push_back(conditions);
    //                                     }
    //                                     position = command.find_first_of(' ');
    //                                     if ((position != -1) && (command.substr(0, 2) == "or" || command.substr(0, 3) == "and")) {
    //                                         conditions.logicalOP = command.substr(0, position);
    //                                         command.erase(0, position + 1);
    //                                         position = command.find_first_of(' ');
    //                                         if (position != -1) {
    //                                             token = command.substr(0, position);
    //                                             command.erase(0, position + 1);
    //                                             if (token.find_first_of('.') != -1) {
    //                                                 token.replace(token.find_first_of('.'), 1, " ");
    //                                                 stringstream istream(token);
    //                                                 SinglyLinkedList<string> tables;
    //                                                 SinglyLinkedList<string> columns;
    //                                                 tables.push_back(table);
    //                                                 columns.push_back(column);
    //                                                 istream >> table >> column;
    //                                                 tables.push_back(table);
    //                                                 columns.push_back(column);
    //                                                 if (table == cond.getvalue(0).table) { // проверка таблицы в where
    //                                                     position = command.find_first_of(' ');
    //                                                     if ((position != -1) && (command[0] == '=')) {
    //                                                         command.erase(0, position + 1);
    //                                                         position = command.find_first_of(' ');
    //                                                         if (position == -1) { // если нет лог. операторов
    //                                                             if (command.find_first_of('.') == -1) { // если просто значение
    //                                                                 conditions.value = command.substr(0, position);
    //                                                                 conditions.check = true;
    //                                                                 command.erase(0, position + 1);
    //                                                                 values.push_back(conditions);
    //                                                                 selectWithLogic(cond, tables, columns, values);
    //                                                             } else { // если столбец
    //                                                                 token = command.substr(0, position);
    //                                                                 token.replace(token.find_first_of('.'), 1, " ");
    //                                                                 command.erase(0, position + 1);
    //                                                                 stringstream stream(token);
    //                                                                 stream >> conditions.table >> conditions.column;
    //                                                                 conditions.check = false;
    //                                                                 values.push_back(conditions);
    //                                                                 selectWithLogic(cond, tables, columns, values);
    //                                                             }
    //                                                         } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //                                                     } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //                                                 } else cout << "Ошибка, таблица в where не совпадает с начальной!" << endl;
    //                                             } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //                                         } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //                                     } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //                                 }
    //                             } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //                         } else cout << "Ошибка, таблица в where не совпадает с начальной!" << endl;
    //                     } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //                 } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //             } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    //         }
    //     } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    // }

    // void select(SinglyLinkedList<Where>& conditions) { // ф-ия обычного селекта
    //     for (int i = 0; i < conditions.size; ++i) {
    //         bool check = checkLockTable(conditions.getvalue(i).table);
    //         if (!check) {
    //             cout << "Ошибка, таблица открыта другим пользователем!" << endl;
    //             return;
    //         }
    //     }
    //     string filepath;
    //     for (int i = 0; i < conditions.size; ++i) {
    //         filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
    //         foutput(filepath, "close");
    //     }

    //     SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов после "select"
    //     SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
    //     sample(stlbindex, tables); // выборка

    //     for (int i = 0; i < conditions.size; ++i) {
    //         filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
    //         foutput(filepath, "open");
    //     }
    // }

    // void selectWithValue(SinglyLinkedList<Where>& conditions, string& table, string& stolbec, struct Where value) { // ф-ия селекта с where для обычного условия
    //     for (int i = 0; i < conditions.size; ++i) {
    //         bool check = checkLockTable(conditions.getvalue(i).table);
    //         if (!check) {
    //             cout << "Ошибка, таблица открыта другим пользователем!" << endl;
    //             return;
    //         }
    //     }
    //     string filepath;
    //     for (int i = 0; i < conditions.size; ++i) {
    //         filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
    //         foutput(filepath, "close");
    //     }

    //     SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов
    //     int stlbindexval = findIndexStlbCond(table, stolbec); // узнаем индекс столбца условия
    //     int stlbindexvalnext = findIndexStlbCond(value.table, value.column); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
    //     SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
    //     SinglyLinkedList<string> column = findStlbTable(conditions, tables, stlbindexvalnext, value.table);; // записываем колонки таблицы условия после '='(нужно если условиестолбец)
        
    //     // фильтруем нужные строки
    //     for (int i = 0; i < conditions.size; ++i) {
    //         if (conditions.getvalue(i).table == table) { 
    //             stringstream stream(tables.getvalue(i));
    //             string str;
    //             string filetext;
    //             int iterator = 0; // нужно для условиястолбец 
    //             while (getline(stream, str)) {
    //                 stringstream istream(str);
    //                 string token;
    //                 int currentIndex = 0;
    //                 while (getline(istream, token, ',')) {
    //                     if (value.check) { // для простого условия
    //                         if (currentIndex == stlbindexval && token == value.value) {
    //                             filetext += str + '\n';
    //                             break;
    //                         }
    //                         currentIndex++;
    //                     } else { // для условиястолбец
    //                         if (currentIndex == stlbindexval && token == column.getvalue(iterator)) {
    //                         filetext += str + '\n';
    //                         }
    //                         currentIndex++;
    //                     }
    //                 }
    //                 iterator++;
    //             }
    //             tables.replace(i, filetext);
    //         }
    //     }

    //     sample(stlbindex, tables); // выборка

    //     for (int i = 0; i < conditions.size; ++i) {
    //         filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
    //         foutput(filepath, "open");
    //     }
    // }

    // void selectWithLogic(SinglyLinkedList<Where>& conditions, SinglyLinkedList<string>& table, SinglyLinkedList<string>& stolbec, SinglyLinkedList<Where>& value) {
    //     for (int i = 0; i < conditions.size; ++i) {
    //         bool check = checkLockTable(conditions.getvalue(i).table);
    //         if (!check) {
    //             cout << "Ошибка, таблица открыта другим пользователем!" << endl;
    //             return;
    //         }
    //     }
    //     string filepath;
    //     for (int i = 0; i < conditions.size; ++i) {
    //         filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
    //         foutput(filepath, "close");
    //     }

    //     SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов после "select"
    //     SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
    //     SinglyLinkedList<int> stlbindexval;// узнаем индексы столбца условия
    //     for (int i = 0; i < stolbec.size; ++i) {
    //         int index = findIndexStlbCond(table.getvalue(i), stolbec.getvalue(i));
    //         stlbindexval.push_back(index);
    //     }
    //     SinglyLinkedList<int> stlbindexvalnext; // узнаем индекс столбца условия после '='(нужно если условиестолбец)
    //     for (int i = 0; i < value.size; ++i) {
    //         int index = findIndexStlbCond(value.getvalue(i).table, value.getvalue(i).column); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
    //         stlbindexvalnext.push_back(index);
    //     }
    //     SinglyLinkedList<string> column;
    //     for (int j = 0; j < value.size; ++j) {
    //         if (!value.getvalue(j).check) { // если условие столбец
    //             column = findStlbTable(conditions, tables, stlbindexvalnext.getvalue(j), value.getvalue(j).table);
    //         }
    //     }

    //     // фильтруем нужные строки
    //     for (int i = 0; i < conditions.size; ++i) {
    //         if (conditions.getvalue(i).table == table.getvalue(0)) {
    //             stringstream stream(tables.getvalue(i));
    //             string str;
    //             string filetext;
    //             int iterator = 0; // нужно для условиястолбец 
    //             while (getline(stream, str)) {
    //                 SinglyLinkedList<bool> checkstr;
    //                 for (int j = 0; j < value.size; ++j) {
    //                     stringstream istream(str);
    //                     string token;
    //                     int currentIndex = 0;
    //                     bool check = false;
    //                     while (getline(istream, token, ',')) {
    //                         if (value.getvalue(j).check) { // если просто условие
    //                             if (currentIndex == stlbindexval.getvalue(j) && token == value.getvalue(j).value) {
    //                                 check = true;
    //                                 break;
    //                             }
    //                             currentIndex++;
    //                         } else { // если условие столбец
    //                             if (currentIndex == stlbindexval.getvalue(j) && token == column.getvalue(iterator)) {
    //                                 check = true;
    //                                 break;
    //                             }
    //                             currentIndex++;
    //                         }
    //                     }
    //                     checkstr.push_back(check);
    //                 }
    //                 if (value.getvalue(1).logicalOP == "and") { // Если оператор И
    //                     if (checkstr.getvalue(0) && checkstr.getvalue(1)) filetext += str + "\n";
    //                 } else { // Если оператор ИЛИ
    //                     if (!checkstr.getvalue(0) && !checkstr.getvalue(1));
    //                     else filetext += str + "\n";
    //                 }
    //                 iterator++;
    //             }
    //             tables.replace(i, filetext);
    //         }
    //     }

    //     sample(stlbindex, tables); // выборка

    //     for (int i = 0; i < conditions.size; ++i) {
    //         filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
    //         foutput(filepath, "open");
    //     }
    // }

    bool checkLockTable(string table) {
        string fin = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        string check = fileread(fin);
        return check == "open"; // Возврат статуса блокировки
    }

    void lockTable(string& table, bool open) {
        string fin = "../" + nameBD + "/" + table + "/" + table + "_lock.txt";
        filerec(fin, open ? "open" : "close");
    }

    // SinglyLinkedList<int> findIndexStlb(SinglyLinkedList<Where>& conditions) { // ф-ия нахождения индекса столбцов(для select)
    //     SinglyLinkedList<int> stlbindex;
    //     for (int i = 0; i < conditions.size; ++i) {
    //         int index = nametables.getindex(conditions.getvalue(i).table);
    //         string str = stlb.getvalue(index);
    //         stringstream ss(str);
    //         int stolbecindex = 0;
    //         while (getline(ss, str, ',')) {
    //             if (str == conditions.getvalue(i).column) {
    //                 stlbindex.push_back(stolbecindex);
    //                 break;
    //             }
    //             stolbecindex++;
    //         }
    //     }
    //     return stlbindex;
    // }

    // int findIndexStlbCond(string table, string stolbec) { // ф-ия нахождения индекса столбца условия(для select)
    //     int index = nametables.getindex(table);
    //     string str = stlb.getvalue(index);
    //     stringstream ss(str);
    //     int stlbindex = 0;
    //     while (getline(ss, str, ',')) {
    //         if (str == stolbec) break;
    //         stlbindex++;
    //     }
    //     return stlbindex;
    // }

    // SinglyLinkedList<string> textInFile(SinglyLinkedList<Where>& conditions) { // ф-ия инпута текста из таблиц(для select)
    //     string filepath;
    //     SinglyLinkedList<string> tables;
    //     for (int i = 0; i < conditions.size; ++i) {
    //         string filetext;
    //         int index = nametables.getindex(conditions.getvalue(i).table);
    //         int iter = 0;
    //         do {
    //             iter++;
    //             filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + to_string(iter) + ".csv";
    //             string text = finput(filepath);
    //             int position = text.find('\n'); // удаляем названия столбцов
    //             text.erase(0, position + 1);
    //             filetext += text + '\n';
    //         } while (iter != fileindex.getvalue(index));
    //         tables.push_back(filetext);
    //     }
    //     return tables;
    // }

    // SinglyLinkedList<string> findStlbTable(SinglyLinkedList<Where>& conditions, SinglyLinkedList<string>& tables, int stlbindexvalnext, string table) { // ф-ия инпута нужных колонок из таблиц для условиястолбец(для select)
    //     SinglyLinkedList<string> column;
    //     for (int i = 0; i < conditions.size; ++i) {
    //         if (conditions.getvalue(i).table == table) {
    //             stringstream stream(tables.getvalue(i));
    //             string str;
    //             while (getline(stream, str)) {
    //                 stringstream istream(str);
    //                 string token;
    //                 int currentIndex = 0;
    //                 while (getline(istream, token, ',')) {
    //                     if (currentIndex == stlbindexvalnext) {
    //                         column.push_back(token);
    //                         break;
    //                     }
    //                     currentIndex++;
    //                 }
    //             }
    //         }
    //     }
    //     return column;
    // }

    // void sample(SinglyLinkedList<int>& stlbindex, SinglyLinkedList<string>& tables) { // ф-ия выборки(для select)
    //    for (int i = 0; i < tables.size - 1; ++i) {
    //         stringstream onefile(tables.getvalue(i));
    //         string token;
    //         while (getline(onefile, token)) {
    //             string needstlb;
    //             stringstream ionefile(token);
    //             int currentIndex = 0;
    //             while (getline(ionefile, token, ',')) {
    //                 if (currentIndex == stlbindex.getvalue(i)) {
    //                     needstlb = token;
    //                     break;
    //                 }
    //                 currentIndex++;
    //             }
    //             stringstream twofile(tables.getvalue(i + 1));
    //             while (getline(twofile, token)) {
    //                 stringstream itwofile(token);
    //                 currentIndex = 0;
    //                 while (getline(itwofile, token, ',')) {
    //                     if (currentIndex == stlbindex.getvalue(i + 1)) {
    //                         cout << needstlb << ' ' << token << endl;
    //                         break;
    //                     }
    //                     currentIndex++;
    //                 }
    //             }
    //         } 
    //     } 
    // }

    void commands(string& command) {
        if (command.substr(0, 11) == "INSERT INTO") {
            command.erase(0, 12);
            Insert(command);
        } else if (command.substr(0, 11) == "DELETE FROM") {
            command.erase(0, 12);
            Delete(command);
        //} else if (command.substr(0, 6) == "SELECT") {
            //command.erase(0, 7);
            //isValidSelect(command);
        } else if (command == "STOP") {
            exit(0);
        } else cout << "Ошибка, неизвестная команда!" << endl; 
    }
};


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