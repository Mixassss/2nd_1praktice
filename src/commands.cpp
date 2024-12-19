#include "../include/json.hpp"
#include "../include/list.h"
#include "../include/commands.h"

    void BaseData::parser() { // ф-ия парсинга
        nlohmann::json objJson;
        ifstream fileinput;
        fileinput.open("../base_date.json");
        fileinput >> objJson;
        fileinput.close();

        if (objJson["name"].is_string()) {
        BD = objJson["name"]; // Парсим каталог 
        } else {
            cout << "Объект каталога не найден!" << endl;
            exit(0);
        }

        rowLimits = objJson["tuples_limit"];

        // парсим подкаталоги
        if (objJson.contains("structure") && objJson["structure"].is_object()) { // проверяем, существование объекта и является ли он объектом
            for (auto elem : objJson["structure"].items()) {
                nametables.pushBack(elem.key());
                
                string kolonki = elem.key() + "_pk_sequence,"; // добавление первичного ключа
                for (auto str : objJson["structure"][elem.key()].items()) {
                    kolonki += str.value();
                    kolonki += ',';
                }
                kolonki.pop_back(); // удаление последней запятой
                stlb.pushBack(kolonki);
                fileindex.pushBack(1);
            }
        } else {
            cout << "Объект подкаталогов не найден!" << endl;
            exit(0);
        }
    }

    void BaseData::createdirect() { // ф-ия формирования директории
        string command;
        command = "mkdir ../" + BD; // каталог
        system(command.c_str());

        for (int i = 0; i < nametables.elementCount; ++i) { // подкаталоги и файлы в них
            command = "mkdir ../" + BD + "/" + nametables.getElementAt(i);
            system(command.c_str());
            string filepath = "../" + BD + "/" + nametables.getElementAt(i) + "/1.csv";
            ofstream file;
            file.open(filepath);
            file << stlb.getElementAt(i) << endl;
            file.close();

            // Блокировка таблицы
            filepath = "../" + BD + "/" + nametables.getElementAt(i) + "/" + nametables.getElementAt(i) + "_lock.txt";
            file.open(filepath);
            file << "open";
            file.close();

            // ключ
            filepath = "../" + BD + "/" + nametables.getElementAt(i) + "/" + nametables.getElementAt(i) + "_pk_sequence.txt";
            file.open(filepath);
            file << "1";
            file.close();
        }
    }


/// Функции для DELETE ///
    void BaseData::isValidDel(string& command) { // ф-ия обработки команды DELETE
        string table, conditions;
        int position = command.find_first_of(' ');
        if (position != -1) {
            table = command.substr(0, position);
            conditions = command.substr(position + 1);
        } else table = command;
        if (nametables.getIndex(table) != -1) { // проверка таблицы
            if (conditions.empty()) { // если нет условий, удаляем все
                delAll(table);
            } else {
                if (conditions.substr(0, 6) == "WHERE ") { // проверка наличия where
                    conditions.erase(0, 6);
                    SinglyLinkedList<Filter> cond;
                    Filter where;
                    position = conditions.find_first_of(' '); ////
                    if (position != -1) { // проверка синтаксиса
                        where.column = conditions.substr(0, position);
                        conditions.erase(0, position+1);
                        int index = nametables.getIndex(table);
                        string str = stlb.getElementAt(index);
                        stringstream ss(str);
                        bool check = false;
                        while (getline(ss, str, ',')) if (str == where.column) check = true;
                        if (check) { // проверка столбца
                            if (conditions[0] == '=' && conditions[1] == ' ') { // проверка синтаксиса
                                conditions.erase(0, 2);
                                position = conditions.find_first_of(' ');
                                if (position == -1) { // если нет лог. оператора
                                    where.value = conditions;
                                    delWithValue(table, where.column, where.value);
                                } else { // если есть логический оператор
                                    where.value = conditions.substr(0, position);
                                    conditions.erase(0, position+1);
                                    cond.pushBack(where);
                                    position = conditions.find_first_of(' ');
                                    if ((position != -1) && (conditions.substr(0, 2) == "OR" || conditions.substr(0, 3) == "AND")) {
                                        where.logicOP = conditions.substr(0, position);
                                        conditions.erase(0, position + 1);
                                        position = conditions.find_first_of(' ');
                                        if (position != -1) {
                                            where.column = conditions.substr(0, position);
                                            conditions.erase(0, position+1);
                                            index = nametables.getIndex(table);
                                            str = stlb.getElementAt(index);
                                            stringstream iss(str);
                                            bool check = false;
                                            while (getline(iss, str, ',')) if (str == where.column) check = true;
                                            if (check) { // проверка столбца
                                                if (conditions[0] == '=' && conditions[1] == ' ') { // проверка синтаксиса
                                                    conditions.erase(0, 2);
                                                    position = conditions.find_first_of(' ');
                                                    if (position == -1) {
                                                        where.value = conditions;
                                                        cond.pushBack(where);
                                                        delWithLogic(cond, table);
                                                    } else cout << "Ошибка, нарушен синтаксис команды4!" << endl;
                                                } else cout << "Ошибка, нарушен синтаксис команды3!" << endl;
                                            } else cout << "Ошибка, нет такого столбца!" << endl;
                                        } else cout << "Ошибка, нарушен синтаксис команды2!" << endl;
                                    } else cout << "Ошибка, нарушен синтаксис команды1!" << endl;
                                }
                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                        } else cout << "Ошибка, нет такого столбца!" << endl;
                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                } else cout << "Ошибка, нарушен синтаксис команды!"<< endl;
            }
        } else cout << "Ошибка, нет такой таблицы!" << endl;
    }

    void BaseData::delAll(string& table) { // ф-ия удаления всех строк таблицы
        string filepath;
        int index = nametables.getIndex(table);
        if (checkLockTable(table)) {
            filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "close");
            
            // очищаем все файлы
            int copy = fileindex.getElementAt(index);
            while (copy != 0) {
                filepath = "../" + BD + "/" + table + "/" + to_string(copy) + ".csv";
                filerec(filepath, "");
                copy--;
            }

            filerec(filepath, stlb.getElementAt(index)+"\n"); // добавляем столбцы в 1.csv

            filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }

    void BaseData::delWithValue(string& table, string& stolbec, string& values) { // ф-ия удаления строк таблицы по значению
        string filepath;
        int index = nametables.getIndex(table);
        if (checkLockTable(table)) {
            filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "close");

            // нахождение индекса столбца в файле
            string str = stlb.getElementAt(index);
            stringstream ss(str);
            int stolbecindex = 0;
            while (getline(ss, str, ',')) {
                if (str == stolbec) break;
                stolbecindex++;
            }

            // удаление строк
            int copy = fileindex.getElementAt(index);
            while (copy != 0) {
                filepath = "../" + BD + "/" + table + "/" + to_string(copy) + ".csv";
                string text = fileread(filepath);
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
                filerec(filepath, filteredlines);
                copy--;
            }

            filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }

    void BaseData::delWithLogic(SinglyLinkedList<Filter>& conditions, string& table) { // ф-ия удаления строк таблицы с логикой
        string filepath;
        int index = nametables.getIndex(table);
        if (checkLockTable(table)) {
            filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "close");

            // нахождение индекса столбцов в файле
            SinglyLinkedList<int> stlbindex;
            for (int i = 0; i < conditions.elementCount; ++i) {
                string str = stlb.getElementAt(index);
                stringstream ss(str);
                int stolbecindex = 0;
                while (getline(ss, str, ',')) {
                    if (str == conditions.getElementAt(i).column) {
                        stlbindex.pushBack(stolbecindex);
                        break;
                    }
                    stolbecindex++;
                }
            }

            // удаление строк
            int copy = fileindex.getElementAt(index);
            while (copy != 0) {
                filepath = "../" + BD + "/" + table + "/" + to_string(copy) + ".csv";
                string text = fileread(filepath);
                stringstream stroka(text);
                string filteredRows;
                while (getline(stroka, text)) {
                    SinglyLinkedList<bool> shouldRemove;
                    for (int i = 0; i < stlbindex.elementCount; ++i) {
                        stringstream iss(text);
                        string token;
                        int currentIndex = 0;
                        bool check = false;
                        while (getline(iss, token, ',')) { 
                            if (currentIndex == stlbindex.getElementAt(i) && token == conditions.getElementAt(i).value) {
                                check = true;
                                break;
                            }
                            currentIndex++;
                        }
                        if (check) shouldRemove.pushBack(true);
                        else shouldRemove.pushBack(false);
                    }
                    if (conditions.getElementAt(1).logicOP == "AND") { // Если оператор И
                        if (shouldRemove.getElementAt(0) && shouldRemove.getElementAt(1));
                        else filteredRows += text + "\n";
                    } else { // Если оператор ИЛИ
                        if (!(shouldRemove.getElementAt(0)) && !(shouldRemove.getElementAt(1))) filteredRows += text + "\n";
                    }
                }
                filerec(filepath, filteredRows);
                copy--;
            }

            filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }


/// Функии для INSERT ///
    void BaseData::isValidInsert(string& command) { 
        string table;
        int position = command.find_first_of(' ');
        if (position != -1) { // проверка синтаксиса
            table = command.substr(0, position);
            command.erase(0, position + 1);
            if (nametables.getIndex(table) != -1) { // проверка таблицы
                if (command.substr(0, 7) == "VALUES ") { // проверка values
                    command.erase(0, 7);
                    position = command.find_first_of(' ');
                    if (position == -1) { // проверка синтаксиса ///////
                        if (command[0] == '(' && command[command.size()-1] == ')') { // проверка синтаксиса скобок и их удаление
                            command.erase(0, 1);
                            command.pop_back();
                            position = command.find(' ');
                            while (position != -1) { // удаление пробелов
                                command.erase(position);
                                position = command.find(' ');
                            }
                            insert(table, command);
                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
            } else cout << "Ошибка, нет такой таблицы!" << endl;
        } else cout << "Ошибка, нарушен синатксис команды" << endl;
    }

    void BaseData::insert(string& table, string& values) { // ф-ия вставки в таблицу
        string filepath = "../" + BD + "/" + table + "/" + table + "_pk_sequence.txt";
        int index = nametables.getIndex(table); // получаем индекс таблицы(aka key)
        string val = fileread(filepath);
        int valint = stoi(val);
        valint++;
        filerec(filepath, to_string(valint));

        if (checkLockTable(table)) {
            filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "close");

            // вставка значений в csv, не забывая про увеличение ключа
            filepath = "../" + BD + "/" + table + "/1.csv";
            int countline = countingLine(filepath);
            int fileid = 1; // номер файла csv
            while (true) {
                if (countline == rowLimits) { // если достигнут лимит, то создаем/открываем другой файл
                    fileid++;
                    filepath = "../" + BD + "/" + table + "/" + to_string(fileid) + ".csv";
                    if (fileindex.getElementAt(index) < fileid) {
                        fileindex.replace(index, fileid);
                    }
                } else break;
                countline = countingLine(filepath);
            }

            fstream file;
            file.open(filepath, ios::app);
            file << val + ',' + values + '\n';
            file.close();

            filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
            filerec(filepath, "open");
            cout << "Команда выполнена!" << endl;
        } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
    }


    // ф-ии селекта
    void BaseData::isValidSelect(string& command) { // ф-ия проверки ввода команды select
        Filter conditions;
        SinglyLinkedList<Filter> cond;

        if (command.find_first_of("FROM") != -1) {
            // работа со столбцами
            while (command.substr(0, 4) != "FROM") {
                string token = command.substr(0, command.find_first_of(' '));
                if (token.find_first_of(',') != -1) token.pop_back(); // удаляем запятую
                command.erase(0, command.find_first_of(' ') + 1);
                if (token.find_first_of('.') != -1) token.replace(token.find_first_of('.'), 1, " ");
                else {
                    cout << "Ошибка, нарушен синтаксис команды!" << endl;
                    return;
                }
                stringstream ss(token);
                ss >> conditions.table >> conditions.column;
                bool check = false;
                int i;
                for (i = 0; i < nametables.elementCount; ++i) { // проверка, сущ. ли такая таблица
                    if (conditions.table == nametables.getElementAt(i)) {
                        check = true;
                        break;
                    }
                }
                if (!check) {
                    cout << "Нет такой таблицы!" << endl;
                    return;
                }
                check = false;
                stringstream iss(stlb.getElementAt(i));
                while (getline(iss, token, ',')) { // проверка, сущ. ли такой столбец
                    if (token == conditions.column) {
                        check = true;
                        break;
                    }
                }
                if (!check) {
                    cout << "Нет такого столбца" << endl;
                    return;
                }
                cond.pushBack(conditions);
            }

            command.erase(0, command.find_first_of(' ') + 1); // скип from

            // работа с таблицами
            int iter = 0;
            while (!command.empty()) { // пока строка не пуста
                string token = command.substr(0, command.find_first_of(' '));
                if (token.find_first_of(',') != -1) {
                    token.pop_back();
                }
                int position = command.find_first_of(' ');
                if (position != -1) command.erase(0, position + 1);
                else command.erase(0);
                if (iter + 1 > cond.elementCount || token != cond.getElementAt(iter).table) {
                    cout << "Ошибка, указаные таблицы не совпадают или их больше!" << endl;
                    return;
                }
                if (command.substr(0, 5) == "WHERE") break; // также заканчиваем цикл если встретился WHERE
                iter++;
            }
            if (command.empty()) {
                select(cond);
            } else {
                if (command.find_first_of(' ') != -1) {
                    command.erase(0, 6);
                    int position = command.find_first_of(' ');
                    if (position != -1) {
                        string token = command.substr(0, position);
                        command.erase(0, position + 1);
                        if (token.find_first_of('.') != -1) {
                            token.replace(token.find_first_of('.'), 1, " ");
                            stringstream ss(token);
                            string table, column;
                            ss >> table >> column;
                            if (table == cond.getElementAt(0).table) { // проверка таблицы в where
                                position = command.find_first_of(' ');
                                if ((position != -1) && (command[0] == '=')) {
                                    command.erase(0, position + 1);
                                    position = command.find_first_of(' ');
                                    if (position == -1) { // если нет лог. операторов
                                        if (command.find_first_of('.') == -1) { // если просто значение
                                            conditions.value = command;
                                            conditions.check = true;
                                            selectWithValue(cond, table, column, conditions);
                                        } else { // если столбец
                                            command.replace(command.find_first_of('.'), 1, " ");
                                            stringstream iss(command);
                                            iss >> conditions.table >> conditions.column;
                                            conditions.check = false;
                                            selectWithValue(cond, table, column, conditions);
                                        }

                                    } else { // если есть лог. операторы
                                        SinglyLinkedList<Filter> values;
                                        token = command.substr(0, position);
                                        command.erase(0, position + 1);
                                        if (token.find_first_of('.') == -1) { // если просто значение
                                            conditions.value = token;
                                            conditions.check = true;
                                            values.pushBack(conditions);
                                        } else { // если столбец
                                            token.replace(token.find_first_of('.'), 1, " ");
                                            stringstream stream(token);
                                            stream >> conditions.table >> conditions.column;
                                            conditions.check = false;
                                            values.pushBack(conditions);
                                        }
                                        position = command.find_first_of(' ');
                                        if ((position != -1) && (command.substr(0, 2) == "OR" || command.substr(0, 3) == "AND")) {
                                            conditions.logicOP = command.substr(0, position);
                                            command.erase(0, position + 1);
                                            position = command.find_first_of(' ');
                                            if (position != -1) {
                                                token = command.substr(0, position);
                                                command.erase(0, position + 1);
                                                if (token.find_first_of('.') != -1) {
                                                    token.replace(token.find_first_of('.'), 1, " ");
                                                    stringstream istream(token);
                                                    SinglyLinkedList<string> tables;
                                                    SinglyLinkedList<string> columns;
                                                    tables.pushBack(table);
                                                    columns.pushBack(column);
                                                    istream >> table >> column;
                                                    tables.pushBack(table);
                                                    columns.pushBack(column);
                                                    if (table == cond.getElementAt(0).table) { // проверка таблицы в where
                                                        position = command.find_first_of(' ');
                                                        if ((position != -1) && (command[0] == '=')) {
                                                            command.erase(0, position + 1);
                                                            position = command.find_first_of(' ');
                                                            if (position == -1) { // если нет лог. операторов
                                                                if (command.find_first_of('.') == -1) { // если просто значение
                                                                    conditions.value = command.substr(0, position);
                                                                    conditions.check = true;
                                                                    command.erase(0, position + 1);
                                                                    values.pushBack(conditions);
                                                                    selectWithLogic(cond, tables, columns, values);
                                                                } else { // если столбец
                                                                    token = command.substr(0, position);
                                                                    token.replace(token.find_first_of('.'), 1, " ");
                                                                    command.erase(0, position + 1);
                                                                    stringstream stream(token);
                                                                    stream >> conditions.table >> conditions.column;
                                                                    conditions.check = false;
                                                                    values.pushBack(conditions);
                                                                    selectWithLogic(cond, tables, columns, values);
                                                                }
                                                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                                    } else cout << "Ошибка, таблица в where не совпадает с начальной!" << endl;
                                                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                            } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                                    }
                                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                            } else cout << "Ошибка, таблица в where не совпадает с начальной!" << endl;
                        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                    } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
                } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
            }
        } else cout << "Ошибка, нарушен синтаксис команды!" << endl;
    }

    void BaseData::select(SinglyLinkedList<Filter>& conditions) { // ф-ия обычного селекта
        for (int i = 0; i < conditions.elementCount; ++i) {
            bool check = checkLockTable(conditions.getElementAt(i).table);
            if (!check) {
                cout << "Ошибка, таблица открыта другим пользователем!" << endl;
                return;
            }
        }
        string filepath;
        for (int i = 0; i < conditions.elementCount; ++i) {
            filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
            filerec(filepath, "close");
        }

        SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов после "select"
        SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
        sample(stlbindex, tables); // выборка

        for (int i = 0; i < conditions.elementCount; ++i) {
            filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
            filerec(filepath, "open");
        }
    }

    void BaseData::selectWithValue(SinglyLinkedList<Filter>& conditions, string& table, string& stolbec, struct Filter value) { // ф-ия селекта с where для обычного условия
        for (int i = 0; i < conditions.elementCount; ++i) {
            bool check = checkLockTable(conditions.getElementAt(i).table);
            if (!check) {
                cout << "Ошибка, таблица открыта другим пользователем!" << endl;
                return;
            }
        }
        string filepath;
        for (int i = 0; i < conditions.elementCount; ++i) {
            filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
            filerec(filepath, "close");
        }

        SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов
        int stlbindexval = findIndexStlbCond(table, stolbec); // узнаем индекс столбца условия
        int stlbindexvalnext = findIndexStlbCond(value.table, value.column); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
        SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
        SinglyLinkedList<string> column = findStlbTable(conditions, tables, stlbindexvalnext, value.table);; // записываем колонки таблицы условия после '='(нужно если условиестолбец)
        
        // фильтруем нужные строки
        for (int i = 0; i < conditions.elementCount; ++i) {
            if (conditions.getElementAt(i).table == table) { 
                stringstream stream(tables.getElementAt(i));
                string str;
                string filetext;
                int iterator = 0; // нужно для условиястолбец 
                while (getline(stream, str)) {
                    stringstream istream(str);
                    string token;
                    int currentIndex = 0;
                    while (getline(istream, token, ',')) {
                        if (value.check) { // для простого условия
                            if (currentIndex == stlbindexval && token == value.value) {
                                filetext += str + '\n';
                                break;
                            }
                            currentIndex++;
                        } else { // для условиястолбец
                            if (currentIndex == stlbindexval && token == column.getElementAt(iterator)) {
                            filetext += str + '\n';
                            }
                            currentIndex++;
                        }
                    }
                    iterator++;
                }
                tables.replace(i, filetext);
            }
        }

        sample(stlbindex, tables); // выборка

        for (int i = 0; i < conditions.elementCount; ++i) {
            filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
            filerec(filepath, "open");
        }
    }

    void BaseData::selectWithLogic(SinglyLinkedList<Filter>& conditions, SinglyLinkedList<string>& table, SinglyLinkedList<string>& stolbec, SinglyLinkedList<Filter>& value) {
        for (int i = 0; i < conditions.elementCount; ++i) {
            bool check = checkLockTable(conditions.getElementAt(i).table);
            if (!check) {
                cout << "Ошибка, таблица открыта другим пользователем!" << endl;
                return;
            }
        }
        string filepath;
        for (int i = 0; i < conditions.elementCount; ++i) {
            filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
            filerec(filepath, "close");
        }

        SinglyLinkedList<int> stlbindex = findIndexStlb(conditions); // узнаем индексы столбцов после "select"
        SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
        SinglyLinkedList<int> stlbindexval;// узнаем индексы столбца условия
        for (int i = 0; i < stolbec.elementCount; ++i) {
            int index = findIndexStlbCond(table.getElementAt(i), stolbec.getElementAt(i));
            stlbindexval.pushBack(index);
        }
        SinglyLinkedList<int> stlbindexvalnext; // узнаем индекс столбца условия после '='(нужно если условиестолбец)
        for (int i = 0; i < value.elementCount; ++i) {
            int index = findIndexStlbCond(value.getElementAt(i).table, value.getElementAt(i).column); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
            stlbindexvalnext.pushBack(index);
        }
        SinglyLinkedList<string> column;
        for (int j = 0; j < value.elementCount; ++j) {
            if (!value.getElementAt(j).check) { // если условие столбец
                column = findStlbTable(conditions, tables, stlbindexvalnext.getElementAt(j), value.getElementAt(j).table);
            }
        }

        // фильтруем нужные строки
        for (int i = 0; i < conditions.elementCount; ++i) {
            if (conditions.getElementAt(i).table == table.getElementAt(0)) {
                stringstream stream(tables.getElementAt(i));
                string str;
                string filetext;
                int iterator = 0; // нужно для условиястолбец 
                while (getline(stream, str)) {
                    SinglyLinkedList<bool> checkstr;
                    for (int j = 0; j < value.elementCount; ++j) {
                        stringstream istream(str);
                        string token;
                        int currentIndex = 0;
                        bool check = false;
                        while (getline(istream, token, ',')) {
                            if (value.getElementAt(j).check) { // если просто условие
                                if (currentIndex == stlbindexval.getElementAt(j) && token == value.getElementAt(j).value) {
                                    check = true;
                                    break;
                                }
                                currentIndex++;
                            } else { // если условие столбец
                                if (currentIndex == stlbindexval.getElementAt(j) && token == column.getElementAt(iterator)) {
                                    check = true;
                                    break;
                                }
                                currentIndex++;
                            }
                        }
                        checkstr.pushBack(check);
                    }
                    if (value.getElementAt(1).logicOP == "AND") { // Если оператор И
                        if (checkstr.getElementAt(0) && checkstr.getElementAt(1)) filetext += str + "\n";
                    } else { // Если оператор ИЛИ
                        if (!checkstr.getElementAt(0) && !checkstr.getElementAt(1));
                        else filetext += str + "\n";
                    }
                    iterator++;
                }
                tables.replace(i, filetext);
            }
        }

        sample(stlbindex, tables); // выборка

        for (int i = 0; i < conditions.elementCount; ++i) {
            filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
            filerec(filepath, "open");
        }
    }

 
    // Вспомогательные ф-ии, чтобы избежать повтора кода в основных ф-иях
    bool BaseData::checkLockTable(string table) { // ф-ия проверки, закрыта ли таблица
        string filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
        string check = fileread(filepath);
        if (check == "open") return true;
        else return false;
    }

    SinglyLinkedList<int> BaseData::findIndexStlb(SinglyLinkedList<Filter>& conditions) { // ф-ия нахождения индекса столбцов(для select)
        SinglyLinkedList<int> stlbindex;
        for (int i = 0; i < conditions.elementCount; ++i) {
            int index = nametables.getIndex(conditions.getElementAt(i).table);
            string str = stlb.getElementAt(index);
            stringstream ss(str);
            int stolbecindex = 0;
            while (getline(ss, str, ',')) {
                if (str == conditions.getElementAt(i).column) {
                    stlbindex.pushBack(stolbecindex);
                    break;
                }
                stolbecindex++;
            }
        }
        return stlbindex;
    }

    int BaseData::findIndexStlbCond(string table, string stolbec) { // ф-ия нахождения индекса столбца условия(для select)
        int index = nametables.getIndex(table);
        string str = stlb.getElementAt(index);
        stringstream ss(str);
        int stlbindex = 0;
        while (getline(ss, str, ',')) {
            if (str == stolbec) break;
            stlbindex++;
        }
        return stlbindex;
    }

    SinglyLinkedList<string> BaseData::textInFile(SinglyLinkedList<Filter>& conditions) { // ф-ия инпута текста из таблиц(для select)
        string filepath;
        SinglyLinkedList<string> tables;
        for (int i = 0; i < conditions.elementCount; ++i) {
            string filetext;
            int index = nametables.getIndex(conditions.getElementAt(i).table);
            int iter = 0;
            do {
                iter++;
                filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + to_string(iter) + ".csv";
                string text = fileread(filepath);
                int position = text.find('\n'); // удаляем названия столбцов
                text.erase(0, position + 1);
                filetext += text + '\n';
            } while (iter != fileindex.getElementAt(index));
            tables.pushBack(filetext);
        }
        return tables;
    }

    SinglyLinkedList<string> BaseData::findStlbTable(SinglyLinkedList<Filter>& conditions, SinglyLinkedList<string>& tables, int stlbindexvalnext, string table) { // ф-ия инпута нужных колонок из таблиц для условиястолбец(для select)
        SinglyLinkedList<string> column;
        for (int i = 0; i < conditions.elementCount; ++i) {
            if (conditions.getElementAt(i).table == table) {
                stringstream stream(tables.getElementAt(i));
                string str;
                while (getline(stream, str)) {
                    stringstream istream(str);
                    string token;
                    int currentIndex = 0;
                    while (getline(istream, token, ',')) {
                        if (currentIndex == stlbindexvalnext) {
                            column.pushBack(token);
                            break;
                        }
                        currentIndex++;
                    }
                }
            }
        }
        return column;
    }

    void BaseData::sample(SinglyLinkedList<int>& stlbindex, SinglyLinkedList<string>& tables) { // ф-ия выборки(для select)
       for (int i = 0; i < tables.elementCount - 1; ++i) {
            stringstream onefile(tables.getElementAt(i));
            string token;
            while (getline(onefile, token)) {
                string needstlb;
                stringstream ionefile(token);
                int currentIndex = 0;
                while (getline(ionefile, token, ',')) {
                    if (currentIndex == stlbindex.getElementAt(i)) {
                        needstlb = token;
                        break;
                    }
                    currentIndex++;
                }
                stringstream twofile(tables.getElementAt(i + 1));
                while (getline(twofile, token)) {
                    stringstream itwofile(token);
                    currentIndex = 0;
                    while (getline(itwofile, token, ',')) {
                        if (currentIndex == stlbindex.getElementAt(i + 1)) {
                            cout << needstlb << ' ' << token << endl;
                            break;
                        }
                        currentIndex++;
                    }
                }
            } 
        } 
    }