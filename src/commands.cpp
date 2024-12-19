#include "../include/json.hpp"
#include "../include/list.h"
#include "../include/commands.h"

void BaseDate::parser() { // ф-ия парсинга
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

    if (objJson.contains("structure") && objJson["structure"].is_object()) {
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

void BaseDate::createdirect() {
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

/// Функии для INSERT ///
void BaseDate::checkInsert(string& table, string& values) {
    string filepath = "../" + BD + "/" + table + "/" + table + "_pk_sequence.txt";
    int index = nametables.getIndex(table); // получаем индекс таблицы
    string val = fileread(filepath);
    int valint = stoi(val);
    valint++;
    filerec(filepath, to_string(valint));

    if (checkLockTable(table)) {
        lockTable(table, false);
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

        lockTable(table, true);
        cout << "Команда выполнена!" << endl;
    } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
}

void BaseDate::Insert(string& command) { 
    size_t position = command.find(' ');
    if (position == string::npos) {
        cout << "Ошибка, нарушен синтаксис команды" << endl;
        return;
    }
    string table = command.substr(0, position);
    command.erase(0, position + 1);
    if (nametables.getIndex(table) == -1) {
        cout << "Ошибка, нет такой таблицы!" << endl;
        return;
    }
    if (command.substr(0, 7) != "VALUES ") {
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
        return;
    }
    command.erase(0, 7);
    if (command.size() < 2 || command.front() != '(' || command.back() != ')') {
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
        return;
    }
    command.erase(0, 1);
    command.pop_back();
    command.erase(remove(command.begin(), command.end(), ' '), command.end());
    checkInsert(table, command);
}

/// Функции для DELETE ///
void BaseDate::delAll(string& table) {
    string filepath;
    int index = nametables.getIndex(table);
    if (checkLockTable(table)) {
        lockTable(table, false);

        int copy = fileindex.getElementAt(index); // очищаем все файлы
        while (copy != 0) {
            filepath = "../" + BD + "/" + table + "/" + to_string(copy) + ".csv";
            filerec(filepath, "");
            copy--;
        }
        filerec(filepath, stlb.getElementAt(index)+"\n"); // добавляем столбцы в 1.csv
        lockTable(table, true);
        cout << "Команда выполнена!" << endl;
    } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
}

void BaseDate::delZnach(string& table, string& stolbec, string& values) {
    string filepath;
    int index = nametables.getIndex(table);
    if (checkLockTable(table)) {
        lockTable(table, false);

        string str = stlb.getElementAt(index); // нахождение индекса столбца в файле
        stringstream ss(str);
        int stolbecindex = 0;
        while (getline(ss, str, ',')) {
            if (str == stolbec) break;
            stolbecindex++;
        }

        int copy = fileindex.getElementAt(index); // удаление строк
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

        lockTable(table, true);
        cout << "Команда выполнена!" << endl;
    } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
}

void BaseDate::delYslov(SinglyLinkedList<Filter>& conditions, string& table) {
    string fin;
    int index = nametables.getIndex(table);
    if (checkLockTable(table)) {
        lockTable(table, false);

        SinglyLinkedList<int> stlbindex; // нахождение индекса столбцов в файле
        for (int i = 0; i < conditions.elementCount; ++i) {
            string str = stlb.getElementAt(index);
            stringstream ss(str);
            int stolbecindex = 0;
            while (getline(ss, str, ',')) {
                if (str == conditions.getElementAt(i).colona) {
                    stlbindex.pushBack(stolbecindex);
                    break;
                }
                stolbecindex++;
            }
        }

        int copy = fileindex.getElementAt(index); // удаление строк
        while (copy != 0) {
            fin = "../" + BD + "/" + table + "/" + to_string(copy) + ".csv";
            string text = fileread(fin);
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
            filerec(fin, filteredRows);
            copy--;
        }
        lockTable(table, true);
        cout << "Команда выполнена!" << endl;
    } else cout << "Ошибка, таблица используется другим пользователем!" << endl;
}

void BaseDate::Delete(string& command) {
    string table, conditions;
    int position = command.find_first_of(' ');

    if (position != -1) {
        table = command.substr(0, position);
        conditions = command.substr(position + 1);
    } else {
        table = command;
    }

    if (nametables.getIndex(table) == -1) {
        cout << "Ошибка, нет такой таблицы!" << endl;
        return;
    }

    if (conditions.empty()) {
        delAll(table);
        return;
    }

    if (conditions.substr(0, 6) != "WHERE ") {
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
        return;
    }

    conditions.erase(0, 6);
    SinglyLinkedList<Filter> cond;
    if (!parseConditions(conditions, table, cond)) {
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
        return;
    }

    if (cond.elementCount == 1) {
        string colona = cond.getElementAt(0).colona;
        string value = cond.getElementAt(0).value;
        delZnach(table, colona, value);
    } else {
        delYslov(cond, table);
    }
}

bool BaseDate::parseConditions(string& conditions, string& table, SinglyLinkedList<Filter>& cond) {
    Filter where;
    int position = conditions.find_first_of(' ');

    if (position == -1) return false;

    where.colona = conditions.substr(0, position);
    if (!isValidColumn(table, where.colona)) return false;

    conditions.erase(0, position + 1);
    if (conditions.substr(0, 2) != "= ") return false;

    conditions.erase(0, 2);
    position = conditions.find_first_of(' ');
    where.value = (position == -1) ? conditions : conditions.substr(0, position);

    cond.pushBack(where);

    if (position == -1) return true;

    conditions.erase(0, position + 1);
    position = conditions.find_first_of(' ');
    if (position == -1 || (conditions.substr(0, 2) != "OR" && conditions.substr(0, 3) != "AND")) return false;

    where.logicOP = conditions.substr(0, position);
    conditions.erase(0, position + 1);

    position = conditions.find_first_of(' ');
    where.colona = (position == -1) ? conditions : conditions.substr(0, position);
    if (!isValidColumn(table, where.colona)) return false;

    conditions.erase(0, position + 1);
    if (conditions.substr(0, 2) != "= ") return false;

    conditions.erase(0, 2);
    where.value = conditions;
    cond.pushBack(where);

    return true;
}

bool BaseDate::isValidColumn(string& table, string& colona) {
    int index = nametables.getIndex(table);
    string str = stlb.getElementAt(index);
    stringstream ss(str);
    string column;

    while (getline(ss, column, ',')) {
        if (column == colona) return true;
    }

    cout << "Ошибка, нет такого столбца!" << endl;
    return false;
}

/// Функции для SELECT ///
void BaseDate::selectall(SinglyLinkedList<Filter>& conditions) {
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

    SinglyLinkedList<int> stlbindex = findIndexColona(conditions); // узнаем индексы столбцов после "select"
    SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
    selection(stlbindex, tables); // выборка

    for (int i = 0; i < conditions.elementCount; ++i) {
        filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
        filerec(filepath, "open");
    }
}

void BaseDate::selectWithValue(SinglyLinkedList<Filter>& conditions, string& table, string& stolbec, struct Filter value) { // ф-ия селекта с where для обычного условия
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

    SinglyLinkedList<int> stlbindex = findIndexColona(conditions); // узнаем индексы столбцов
    int stlbindexval = findIndexStlbCond(table, stolbec); // узнаем индекс столбца условия
    int stlbindexvalnext = findIndexStlbCond(value.table, value.colona); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
    SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
    SinglyLinkedList<string> column = InputTable(conditions, tables, stlbindexvalnext, value.table);; // записываем колонки таблицы условия после '='(нужно если условиестолбец)
        
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
    selection(stlbindex, tables); // выборка
    for (int i = 0; i < conditions.elementCount; ++i) {
        filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
        filerec(filepath, "open");
    }
}

void BaseDate::selectWithLogic(SinglyLinkedList<Filter>& conditions, SinglyLinkedList<string>& table, SinglyLinkedList<string>& stolbec, SinglyLinkedList<Filter>& value) {
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

    SinglyLinkedList<int> stlbindex = findIndexColona(conditions); // узнаем индексы столбцов после "select"
    SinglyLinkedList<string> tables = textInFile(conditions); // записываем данные из файла в переменные для дальнейшей работы
    SinglyLinkedList<int> stlbindexval;// узнаем индексы столбца условия
    for (int i = 0; i < stolbec.elementCount; ++i) {
        int index = findIndexStlbCond(table.getElementAt(i), stolbec.getElementAt(i));
        stlbindexval.pushBack(index);
    }
    SinglyLinkedList<int> stlbindexvalnext; // узнаем индекс столбца условия после '='(нужно если условиестолбец)
    for (int i = 0; i < value.elementCount; ++i) {
        int index = findIndexStlbCond(value.getElementAt(i).table, value.getElementAt(i).colona); // узнаем индекс столбца условия после '='(нужно если условиестолбец)
        stlbindexvalnext.pushBack(index);
    }
    SinglyLinkedList<string> column;
    for (int j = 0; j < value.elementCount; ++j) {
        if (!value.getElementAt(j).check) { // если условие столбец
            column = InputTable(conditions, tables, stlbindexvalnext.getElementAt(j), value.getElementAt(j).table);
        }
    }

    for (int i = 0; i < conditions.elementCount; ++i) { // фильтруем нужные строки
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
    selection(stlbindex, tables); // выборка
    for (int i = 0; i < conditions.elementCount; ++i) {
        filepath = "../" + BD + '/' + conditions.getElementAt(i).table + '/' + conditions.getElementAt(i).table + "_lock.txt";
        filerec(filepath, "open");
    }
}

/// Вспомогательные функции ///
bool BaseDate::checkLockTable(string table) { // ф-ия проверки, закрыта ли таблица
    string filepath = "../" + BD + "/" + table + "/" + table + "_lock.txt";
    string check = fileread(filepath);
    if (check == "open") return true;
    else return false;
}

void BaseDate::lockTable(string& table, bool open) {
    string fin = "../" + BD + "/" + table + "/" + table + "_lock.txt";
    filerec(fin, open ? "open" : "close");
}

SinglyLinkedList<int> BaseDate::findIndexColona(SinglyLinkedList<Filter>& conditions) { // ф-ия нахождения индекса столбцов
    SinglyLinkedList<int> stlbindex;
    for (int i = 0; i < conditions.elementCount; ++i) {
        int index = nametables.getIndex(conditions.getElementAt(i).table);
        string str = stlb.getElementAt(index);
        stringstream ss(str);
        int stolbecindex = 0;
        while (getline(ss, str, ',')) {
            if (str == conditions.getElementAt(i).colona) {
                stlbindex.pushBack(stolbecindex);
                break;
            }
            stolbecindex++;
        }
    }
    return stlbindex;
}

int BaseDate::findIndexStlbCond(string table, string stolbec) { // ф-ия нахождения индекса столбца условия
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

SinglyLinkedList<string> BaseDate::textInFile(SinglyLinkedList<Filter>& conditions) { // ф-ия инпута текста из таблиц
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

SinglyLinkedList<string> BaseDate::InputTable(SinglyLinkedList<Filter>& conditions, SinglyLinkedList<string>& tables, int stlbindexvalnext, string table) { // ф-ия инпута нужных колонок из таблиц для условиястолбец(для select)
    SinglyLinkedList<string> colona;
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
                        colona.pushBack(token);
                        break;
                    }
                    currentIndex++;
                }
            }
        }
    }
    return colona;
}

void BaseDate::selection(SinglyLinkedList<int>& stlbindex, SinglyLinkedList<string>& tables) {
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

void BaseDate::Select(string& command) { // ф-ия проверки ввода команды select
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
            ss >> conditions.table >> conditions.colona;
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
                if (token == conditions.colona) {
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
            selectall(cond);
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
                                        iss >> conditions.table >> conditions.colona;
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
                                        stream >> conditions.table >> conditions.colona;
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
                                                                stream >> conditions.table >> conditions.colona;
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