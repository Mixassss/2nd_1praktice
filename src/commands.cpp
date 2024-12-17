#include "../include/commands.h"

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

/// Функции для инсерта ///
void BaseData::checkInsert(string& table, string& values) {
    if (!checkLockTable(table)) { // Проверка блокировки таблицы
        cerr << "Ошибка! Таблица " << table << " заблокирована другим пользователем!" << endl;
        return;
    }
    string pkFilePath = "../" + BD + "/" + table + "/" + table + "_pk_sequence.txt"; // Получение и обновление первичного ключа
    string pkValue = fileread(pkFilePath);
    if (pkValue.empty()) {
        cerr << "Ошибка! Не удалось прочитать значение первичного ключа!" << endl;
        return;
    }
    int pkInt = stoi(pkValue);
    filerec(pkFilePath, to_string(pkInt + 1));
    int fileCount; // Получение количества файлов таблицы
    if (!fileCountHash.get(table, fileCount)) {
        cerr << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
        return;
    }
    string csvFilePath = "../" + BD + "/" + table + "/" + to_string(fileCount) + ".csv"; // Проверка лимита строк и обновление пути к файлу
    if (countingLine(csvFilePath) >= rowLimits) {
        fileCountHash.insert(table, ++fileCount);
        csvFilePath = "../" + BD + "/" + table + "/" + to_string(fileCount) + ".csv";
    }
    ofstream csvFile(csvFilePath, ios::app); // Открытие файла для записи
    if (!csvFile.is_open()) {
        cerr << "Ошибка с открытием файла " << csvFilePath << " для записи!" << endl;
        return;
    }
    if (countingLine(csvFilePath) == 0) { // Запись заголовков в файл, если он пуст
        string columnString;
        if (!coloumnHash.get(table, columnString)) {
            cerr << "Ошибка! Не удалось получить названия столбцов для таблицы " << table << endl;
            return;
        }
        csvFile << columnString << endl;
    }
    csvFile << pkInt << ',' << values << '\n'; // Запись данных с первичным ключом
    csvFile.close();
    cout << "Команда выполнена успешно!" << endl;
}

void BaseData::Insert(string& command) {
    string valuesPrefix = "VALUES";
    size_t position = command.find_first_of(' ');

    if (position == string::npos) {
        cout << "Ошибка! Синтаксис команды нарушен!" << endl;
        return;
    }
    string table = command.substr(0, position); // Извлечение названия таблицы
    command.erase(0, position + 1); // Удаление названия таблицы из команды
    table.erase(remove_if(table.begin(), table.end(), ::isspace), table.end()); // Удаление пробелов до и после названия таблицы

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

/// Функции для DELETE FROM ///
void BaseData::delAll(string& table) {
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
        fin = "../" + BD + "/" + table + "/1.csv"; // Получаем заголовки из 1.csv
        ifstream headerFile(fin);
        if (headerFile.is_open()) {
            getline(headerFile, columnString); // Читаем заголовки
            headerFile.close();
            } else {
                cerr << "Не удалось открыть файл для чтения заголовков!" << endl;
                return;
            }
            for (int all = 1; all <= fileCount; ++all) { // Цикл для очистки всех файлов таблицы, кроме 1.csv
                fin = "../" + BD + "/" + table + "/" + to_string(all) + ".csv";
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

void BaseData::deleteZnach(string& table, string& stolbec, string& values) {
    string fin;
    int index = tablesname.getIndex(table);
    if (checkLockTable(table)) {
        lockTable(table, false);
        
        string str; // Получаем название столбцов из хэш-таблицы
        if (!coloumnHash.get(table, str)) {
            cout << "Ошибка: Столбец не найден!" << endl;
            lockTable(table, true); // Разблокируем таблицу перед выходом
            return;
        }
        stringstream ss(str); // Получаем индекс столбца в файле
        int stolbecindex = 0;
        while (getline(ss, str, ',')) {
            if (str == stolbec) break;
            stolbecindex++;
        }
        int copy; // Получаем количество файлов для данной таблицы
        if (!fileCountHash.get(table, copy)) {
            cout << "Ошибка: Не удалось получить количество файлов для таблицы!" << endl;
            lockTable(table, true); // Разблокируем таблицу перед выходом
            return;
        }

        // Удаление строк
        while (copy != 0) {
            fin = "../" + BD + "/" + table + "/" + to_string(copy) + ".csv";
            string text = fileread(fin);
            stringstream stroka(text);
            string filteredlines;
            while (getline(stroka, text)) {
                stringstream iss(text);
                string token;
                int currentIndex = 0;
                bool shouldRemove = false;
                while (getline(iss, token, ',')) {
                    if (currentIndex == stolbecindex && token == values) {
                        shouldRemove = true;
                        break;
                    }
                    currentIndex++;
                }
                if (!shouldRemove) filteredlines += text + "\n"; 
            }
            filerec(fin, filteredlines);
            copy--;
        }
        lockTable(table, true);
        cout << "Команда выполнена!" << endl;
    } else {
        cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }
}

void BaseData::deleteFilter(Hash_table<string, Filters>& filter, string& table) {
    string filepath;
    int index = tablesname.getIndex(table);
    if (checkLockTable(table)) {
        lockTable(table, false);
        
        Hash_table<string, int> columnIndices; // Нахождение индексов столбцов для фильтров
        string str;
        if (!coloumnHash.get(table, str)) {
            cout << "Ошибка: Не удалось получить значения столбцов для таблицы " << table << endl;
            return;
        }
        stringstream ss(str);
        string columnName;
        int columnIndex = 0;
        while (getline(ss, columnName, ',')) {
            columnIndices.insert(columnName, columnIndex);
            columnIndex++;
        }
        int copy;
        if (!fileCountHash.get(table, copy)) {
            cout << "Ошибка: Не удалось получить количество файлов для таблицы " << table << endl;
            return;
        }
        bool anyRowDeleted = false;
        while (copy != 0) {
            filepath = "../" + BD + "/" + table + "/" + to_string(copy) + ".csv";
            string text = fileread(filepath);
            stringstream stroka(text);
            string filteredRows;
            string line;

            while (getline(stroka, line)) {
                bool shouldRemove = false;
                for (int i = 0; i < filter.size(); ++i) {
                    Filters filterValue = filter.getValueAt(i);
                    int columnIdx; // Проверяем наличие индекса столбца для текущего фильтра
                    if (!columnIndices.get(filterValue.colona, columnIdx)) {
                        cout << "Ошибка: Не удалось найти столбец " << filterValue.colona << endl;
                        return;
                    }
                    stringstream iss(line);
                    string token;
                    int currentIndex = 0;
                    bool checkCondition = false;
                    while (getline(iss, token, ',')) {
                        if (currentIndex == columnIdx) {
                            checkCondition = (token == filterValue.value);
                            break;
                        }
                        currentIndex++;
                    }

                    if (filterValue.logicOP == "AND") { // Применение логических операторов
                        shouldRemove = shouldRemove && checkCondition;
                    } else if (filterValue.logicOP == "OR") {
                        shouldRemove = shouldRemove && !checkCondition;
                    } else {
                        shouldRemove = checkCondition; // Для первого условия
                    }
                }
                if (shouldRemove) { // Если результат всех условий true, строка должна быть удалена
                    anyRowDeleted = true;
                } else {
                    filteredRows += line + "\n";
                }
            }
            if (!filteredRows.empty()) {
                filteredRows.pop_back(); // Убираем последний символ новой строки
                filerec(filepath, filteredRows);
            }
            copy--;
        }
        if (anyRowDeleted) {
            cout << "Команда выполнена!" << endl;
        } else {
            cout << "Ошибка: Не найдено строк для удаления." << endl;
        }
        lockTable(table, true);
    } else {
        cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }
}

void BaseData::Delete(string& command) {
    string table, conditions;
    int position = command.find_first_of(' ');
    if (position != -1) { // Разделение команды на таблицу и условия
        table = command.substr(0, position);
        conditions = command.substr(position + 1);
    } else {
        table = command;
    }
    int fileCount; // Проверка существования таблицы в хэш-таблице
    if (!fileCountHash.get(table, fileCount)) {
        cout << "Ошибка, нет такой таблицы!" << endl;
        return;
    }
    if (conditions.empty()) { // Удаление всех записей, если нет условий
        delAll(table);
        return;
    }
    if (conditions.substr(0, 6) != "WHERE ") { // Проверка условий
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
        return;
    }
    conditions.erase(0, 6); // Удаляем "WHERE "
    Hash_table<string, Filters> yslov; // Хэш-таблица для условий
    Filters filter;
    position = conditions.find_first_of(' '); // Обработка первого условия
    if (position == -1) {
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
        return;
    }
    filter.colona = conditions.substr(0, position);
    conditions.erase(0, position + 1);

    // Проверка наличия столбца
    string columnString;
    if (!coloumnHash.get(table, columnString) || !isColumnValid(columnString, filter.colona)) {
        cout << "Ошибка, нет такого столбца!" << endl;
        return;
    }

    // Проверка синтаксиса условия
    if (conditions.substr(0, 2) != "= ") {
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
        return;
    }
    conditions.erase(0, 2);
    position = conditions.find_first_of(' ');

    if (position == -1) {
        filter.value = conditions;
        deleteZnach(table, filter.colona, filter.value);
        return;
    }

    filter.value = conditions.substr(0, position);
    conditions.erase(0, position + 1);
    yslov.insert(filter.colona, filter);
    if (!processLogicalOperator(conditions, yslov, table)) { // Обработка логического оператора
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
    } else {
        deleteFilter(yslov, table);
    }
}

bool BaseData::isColumnValid(const string& columnString, const string& column) { // Проверка наличия столбца в строке
    stringstream ss(columnString);
    string str;
    while (getline(ss, str, ',')) {
        str.erase(remove_if(str.begin(), str.end(), ::isspace), str.end());
        if (str == column) {
            return true;
        }
    }
    return false;
}

bool BaseData::processLogicalOperator(string& conditions, Hash_table<string, Filters>& yslov, const string& table) { // Обработка логического оператора
    int position = conditions.find_first_of(' ');
    if (position == -1 || (conditions.substr(0, 2) != "OR" && conditions.substr(0, 3) != "AND")) {
        return false;
    }
    Filters filter;
    filter.logicOP = conditions.substr(0, position);
    conditions.erase(0, position + 1);
    position = conditions.find_first_of(' ');
    if (position == -1) {
        return false;
    }
    filter.colona = conditions.substr(0, position);
    conditions.erase(0, position + 1);
    string columnString; // Проверка наличия второго столбца
    if (!coloumnHash.get(table, columnString) || !isColumnValid(columnString, filter.colona)) {
        cout << "Ошибка, нет такого столбца!" << endl;
        return false;
    }
    if (conditions.substr(0, 2) != "= ") {
        return false;
    }
    conditions.erase(0, 2);
    filter.value = conditions;
    yslov.insert(filter.colona, filter);
    return true;
}

/// Функции для SELECTA ///
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

void BaseData::select(Hash_table<string, Filters>& filter) { // Функция селекта
    for (int i = 0; i < filter.size(); ++i) { // Проверка блокировки таблиц
        Filters currentFilter;
        if (!filter.get(filter.getKeyAt(i), currentFilter)) {
            throw runtime_error("Фильтр не найден по ключу");
        }
        bool check = checkLockTable(currentFilter.table);
        if (!check) {
            cout << "Ошибка, таблица открыта другим пользователем!" << endl;
            return;
        }
    }

    for (int i = 0; i < filter.size(); ++i) { // Закрытие таблиц
        Filters currentFilter;
        if (!filter.get(filter.getKeyAt(i), currentFilter)) {
            throw runtime_error("Фильтр не найден по ключу");
        }
        string filepath = "../" + BD + '/' + currentFilter.table + '/' + currentFilter.table + "_lock.txt";
        filerec(filepath, "close");
    }

    Hash_table<int, string> stlbindex; // Узнаем индексы столбцов "select" и записываем данные из файла
    for (int i = 0; i < filter.size(); ++i) {
        Filters currentFilter;
        if (!filter.get(filter.getKeyAt(i), currentFilter)) {
            throw runtime_error("Фильтр не найден по ключу");
        }
        int index = tablesname.getIndex(currentFilter.table);
        string str = coloumnHash.getValueAt(index); // Используем новый метод
        stringstream ss(str);
        int stolbecindex = 0;
        while (getline(ss, str, ',')) {
            if (str == currentFilter.colona) {
                stlbindex.insert(i, to_string(stolbecindex)); // Добавляем индекс как строку
                break;
            }
            stolbecindex++;
        }
    }

    Hash_table<string, string> stringTables = textInput(filter);
    Hash_table<int, string> intTables;

    for (int i = 0; i < stringTables.size(); ++i) { // Преобразуем stringTables в intTables
        string key = stringTables.getKeyAt(i);
        string value;
        if (!stringTables.get(key, value)) {
            throw runtime_error("Не удалось получить значение для ключа: " + key);
        }
        intTables.insert(i, value); // Используем числовой индекс
    }
    sample(stlbindex, intTables); // Выполнение выборки
    for (int i = 0; i < filter.size(); ++i) {  // Открытие таблиц после работы
        Filters currentFilter;
        if (!filter.get(filter.getKeyAt(i), currentFilter)) {
            throw runtime_error("Фильтр не найден по ключу");
        }
        string filepath = "../" + BD + '/' + currentFilter.table + '/' + currentFilter.table + "_lock.txt";
        filerec(filepath, "open");
    }
}

void BaseData::selectWithValue(SinglyLinkedList<Filters>& filtersList, string& table, string& column, Filters value) {
    Hash_table<string, Filters> filtersHash; // Преобразование SinglyLinkedList в Hash_table
    for (int i = 0; i < filtersList.size(); ++i) {
        Filters filter = filtersList.getElementAt(i);
        filtersHash.insert(filter.table, filter);
    }

    // Проверка блокировки таблиц
    for (int i = 0; i < filtersHash.size(); ++i) {
        Filters filter;
        filtersHash.get(filtersHash.getKeyAt(i), filter);
        if (!checkLockTable(filter.table)) {
            cout << "Ошибка, таблица открыта другим пользователем!" << endl;
            return;
        }
    }

    // Закрытие таблиц для работы
    for (int i = 0; i < filtersHash.size(); ++i) {
        Filters filter;
        filtersHash.get(filtersHash.getKeyAt(i), filter);
        string filepath = "../" + BD + '/' + filter.table + '/' + filter.table + "_lock.txt";
        filerec(filepath, "close");
    }

    // Поиск индексов столбцов
    Hash_table<string, int> stlbIndexTemp = findIndexStlb(filtersHash); // Получаем string -> int
    Hash_table<int, string> stlbIndex; // Для sample требуется int -> string
    for (int i = 0; i < stlbIndexTemp.size(); ++i) {
        stlbIndex.insert(stlbIndexTemp.getValueAt(i), stlbIndexTemp.getKeyAt(i));
    }

    int stlbIndexVal = findIndexStlbCond(table, column);
    int stlbIndexValNext = findIndexStlbCond(value.table, value.colona);

    // Чтение данных таблиц
    Hash_table<string, string> tables = textInput(filtersHash);

    // Условные столбцы для фильтрации
    Hash_table<string, string> conditionColumns = findTable(filtersList, tables, stlbIndexValNext, value.table);

    // Фильтрация строк
    for (int i = 0; i < filtersHash.size(); ++i) {
        Filters filter;
        filtersHash.get(filtersHash.getKeyAt(i), filter);

        if (filter.table == table) {
            string tableData;
            if (tables.get(filter.table, tableData)) {
                stringstream stream(tableData);
                string str, fileText;
                int iterator = 0;

                while (getline(stream, str)) {
                    stringstream istream(str);
                    string token;
                    int currentIndex = 0;

                    while (getline(istream, token, ',')) {
                        if (value.check) { // Простое условие
                            if (currentIndex == stlbIndexVal && token == value.value) {
                                fileText += str + '\n';
                                break;
                            }
                        } else { // Условие-столбец
                            if (currentIndex == stlbIndexVal && token == conditionColumns.getValueAt(iterator)) {
                                fileText += str + '\n';
                                break;
                            }
                        }
                        currentIndex++;
                    }
                    iterator++;
                }
                tables.insert(filter.table, fileText);
            }
        }
    }

    // Преобразование Hash_table<string, string> в Hash_table<int, string>
    Hash_table<int, string> indexedTables;
    for (int i = 0; i < tablesname.size(); ++i) {
        string tableName = tablesname.getElementAt(i);
        string tableData;
        if (tables.get(tableName, tableData)) {
            indexedTables.insert(i, tableData);
        }
    }
    sample(stlbIndex, indexedTables); // Выборка данных

    // Открытие таблиц обратно
    for (int i = 0; i < filtersHash.size(); ++i) {
        Filters filter;
        filtersHash.get(filtersHash.getKeyAt(i), filter);
        string filepath = "../" + BD + '/' + filter.table + '/' + filter.table + "_lock.txt";
        filerec(filepath, "open");
    }
}

void BaseData::selectWithLogic(SinglyLinkedList<Filters>& conditions, SinglyLinkedList<string>& table, SinglyLinkedList<string>& stolbec, SinglyLinkedList<Filters>& value) {
    for (int i = 0; i < conditions.size(); ++i) {
        bool check = checkLockTable(conditions.getvalue(i).table);
        if (!check) {
            cout << "Ошибка, таблица открыта другим пользователем!" << endl;
            return;
        }
    }
         string filepath;
         for (int i = 0; i < conditions.size; ++i) {
             filepath = "../" + nameBD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
             foutput(filepath, "close");
         }

         SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов после "select"
         SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
         SinglyLinkedList<int> stlbindexval;// узнаем индексы столбца условия
         for (int i = 0; i < stolbec.size; ++i) {
             int index = findIndexStlbCond(table.getvalue(i), stolbec.getvalue(i));
             stlbindexval.push_back(index);
         }
         SinglyLinkedList<int> stlbindexvalnext; // узнаем индекс столбца условия после '='(нужно если условиестолбец)
         for (int i = 0; i < value.size; ++i) {
             int index = findIndexStlbCond(value.getvalue(i).table, value.getvalue(i).column); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
             stlbindexvalnext.push_back(index);
         }
         SinglyLinkedList<string> column;
         for (int j = 0; j < value.size; ++j) {
             if (!value.getvalue(j).check) { // если условие столбец
                 column = findStlbTable(conditions, tables, stlbindexvalnext.getvalue(j), value.getvalue(j).table);
             }
         }

         // фильтруем нужные строки
         for (int i = 0; i < conditions.size; ++i) {
             if (conditions.getvalue(i).table == table.getvalue(0)) {
                 stringstream stream(tables.getvalue(i));
                 string str;
                 string filetext;
                 int iterator = 0; // нужно для условиястолбец 
                 while (getline(stream, str)) {
                     SinglyLinkedList<bool> checkstr;
                     for (int j = 0; j < value.size; ++j) {
                         stringstream istream(str);
                         string token;
                         int currentIndex = 0;
                         bool check = false;
                         while (getline(istream, token, ',')) {
                             if (value.getvalue(j).check) { // если просто условие
                                 if (currentIndex == stlbindexval.getvalue(j) && token == value.getvalue(j).value) {
                                     check = true;
                                     break;
                                }
                                 currentIndex++;
                            } else { // если условие столбец
                                if (currentIndex == stlbindexval.getvalue(j) && token == column.getvalue(iterator)) {
                                     check = true;
                                   break;
                                }
                                currentIndex++;
                            }
                        }
                      checkstr.push_back(check);
                   }
                   if (value.getvalue(1).logicalOP == "and") { // Если оператор И
                        if (checkstr.getvalue(0) && checkstr.getvalue(1)) filetext += str + "\n";
                    } else { // Если оператор ИЛИ
                         if (!checkstr.getvalue(0) && !checkstr.getvalue(1));
                         else filetext += str + "\n";
                    }
                    iterator++;
                }
                 tables.replace(i, filetext);
             }
         }

         sample(stlbindex, tables); // выборка

         for (int i = 0; i < conditions.size(); ++i) {
            filepath = "../" + BD + '/' + conditions.getvalue(i).table + '/' + conditions.getvalue(i).table + "_lock.txt";
            foutput(filepath, "open");
         }
    }

bool BaseData::checkLockTable(string table) {
    string fin = "../" + BD + "/" + table + "/" + table + "_lock.txt";
    string check = fileread(fin);
    return check == "open"; // Возврат статуса блокировки
}

void BaseData::lockTable(string& table, bool open) {
    string fin = "../" + BD + "/" + table + "/" + table + "_lock.txt";
    filerec(fin, open ? "open" : "close");
}

Hash_table<string, int> BaseData::findIndexStlb(Hash_table<string, Filters>& filters) {
    Hash_table<string, int> stlbIndex;
    for (int i = 0; i < filters.size(); ++i) {
        Filters filter;
        filters.get(filters.getKeyAt(i), filter);

        int tableIndex = tablesname.getIndex(filter.table);
        string tableSchema = coloumnHash.getValueAt(tableIndex);

        stringstream ss(tableSchema);
        string columnName;
        int columnIndex = 0;

        while (getline(ss, columnName, ',')) {
            if (columnName == filter.colona) {
                stlbIndex.insert(filter.colona, columnIndex);
                break;
            }
            columnIndex++;
        }
    }
    return stlbIndex;
}

int BaseData::findIndexStlbCond(string table, string stolbec) { // ф-ия нахождения индекса столбца условия(для select)
    int index = tablesname.getIndex(table);
    string str = coloumnHash.getValueAt(index);
    stringstream ss(str);
    int stlbindex = 0;
    while (getline(ss, str, ',')) {
        if (str == stolbec) break;
        stlbindex++;
    }
    return stlbindex;
}

Hash_table<string, string> BaseData::textInput(Hash_table<string, Filters>& filters) { // ф-ия инпута текста из таблиц(для select)
    string fin;
    Hash_table<string, string> tables; // Хэш-таблица для хранения текста из файлов
    for (int i = 0; i < filters.size(); ++i) {
        string filetext;
        string tableName = filters.getKeyAt(i); // Получаем имя таблицы из условий
        int index = tablesname.getIndex(tableName);
        int fileCount = 0; // Переменная для хранения количества файлов
        if (!fileCountHash.get(tableName, fileCount)) {
            throw runtime_error("Не удалось получить количество файлов для таблицы: " + tableName);
        }
        for (int iter = 1; iter <= fileCount; ++iter) {
            fin = "../" + BD + '/' + tableName + '/' + to_string(iter) + ".csv";
            string text = fileread(fin);
            int position = text.find('\n'); // Удаляем названия столбцов
            text.erase(0, position + 1);
            filetext += text + '\n';
        }
        tables.insert(tableName, filetext); // Сохраняем текст в хэш-таблицу с именем таблицы как ключ
    }
    return tables;
}

Hash_table<string, string> BaseData::findTable(SinglyLinkedList<Filters>& filters, Hash_table<string, string>& tablesHash, int stlbindexvalnext, string table) {
    Hash_table<string, string> columnHash;
    for (int i = 0; i < filters.size(); ++i) {
        const Filters& filter = filters.getElementAt(i);
        if (filter.table == table) {
            string tableData;
            if (tablesHash.get(table, tableData)) { // Получаем данные таблицы из хеш-таблицы
                stringstream stream(tableData);
                string row;
                while (getline(stream, row)) {
                    stringstream rowStream(row);
                    string token;
                    int currentIndex = 0;
                    while (getline(rowStream, token, ',')) {
                        if (currentIndex == stlbindexvalnext) {
                            columnHash.insert(token, token); // Используем значение как ключ и значение
                            break;
                        }
                        currentIndex++;
                    }
                }
            }
        }
    }
    return columnHash;
}

void BaseData::sample(Hash_table<int, string>& stlbindex, Hash_table<int, string>& tables) {
    for (int i = 0; i < tables.size() - 1; ++i) {
        string onefile;
        if (!tables.get(i, onefile)) {
            cerr << "Ошибка: таблица с индексом " << i << " не найдена." << endl;
            continue;
        }
        stringstream onefileStream(onefile);
        string token;
        while (getline(onefileStream, token)) {
            string needstlb;
            stringstream lineStream(token);
            int currentIndex = 0;
            while (getline(lineStream, token, ',')) {
                string indexStr;
                if (!stlbindex.get(i, indexStr)) {
                    cerr << "Ошибка: индекс столбца для таблицы " << i << " не найден." << endl;
                    break;
                }
                int index = stoi(indexStr); // Преобразование строки в число
                if (currentIndex == index) {
                    needstlb = token;
                    break;
                }
                currentIndex++;
            }
            string twofile;
            if (!tables.get(i + 1, twofile)) {
                cerr << "Ошибка: таблица с индексом " << i + 1 << " не найдена." << endl;
                continue;
            }
            stringstream twofileStream(twofile);
            while (getline(twofileStream, token)) {
                stringstream lineTwoStream(token);
                currentIndex = 0;
                while (getline(lineTwoStream, token, ',')) {
                    string indexStr;
                    if (!stlbindex.get(i + 1, indexStr)) {
                        cerr << "Ошибка: индекс столбца для таблицы " << i + 1 << " не найден." << endl;
                        break;
                    }
                    int index = stoi(indexStr); // Преобразование строки в число
                    if (currentIndex == index) {
                        cout << needstlb << ' ' << token << endl;
                        break;
                    }
                    currentIndex++;
                }
            }
        }
    }
}