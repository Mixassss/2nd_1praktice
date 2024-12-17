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
        // Путь к файлу первичного ключа
    string pkFilePath = "../" + BD + "/" + table + "/" + table + "_pk_sequence.txt";

    // Чтение и обновление первичного ключа
    string pkValue = fileread(pkFilePath);
    int pkInt = (pkValue.empty()) ? 1 : stoi(pkValue); // Начальное значение ключа
    filerec(pkFilePath, to_string(pkInt + 1));

    // Проверка блокировки таблицы
    if (!checkLockTable(table)) {
        cout << "Ошибка: таблица '" << table << "' заблокирована другим пользователем!" << endl;
        return;
    }

    // Блокируем таблицу для записи
    lockTable(table, false);

    // Работа с файлами и строками
    int fileID = 1;
    int lineCount = 0;
    string csvFilePath = "../" + BD + "/" + table + "/" + to_string(fileID) + ".csv";

    // Получаем количество файлов для таблицы
    int fileCount = 0;
    if (!fileCountHash.get(table, fileCount)) {
        fileCount = 1;
        fileCountHash.insert(table, fileCount);
    }

    // Ищем файл для записи
    while (true) {
        lineCount = countingLine(csvFilePath);
        if (lineCount < rowLimits) {
            break; // Если в файле есть место, завершаем поиск
        }
        fileID++;
        csvFilePath = "../" + BD + "/" + table + "/" + to_string(fileID) + ".csv";

        if (fileID > fileCount) {
            fileCountHash.insert(table, fileID); // Обновляем количество файлов в хеш-таблице
            break;
        }
    }

    // Открываем файл и записываем данные
    ofstream outFile(csvFilePath, ios::app);
    if (!outFile.is_open()) {
        cerr << "Ошибка: не удалось открыть файл '" << csvFilePath << "' для записи!" << endl;
        lockTable(table, true); // Разблокируем таблицу
        return;
    }

    if (lineCount == 0) { // Если файл пустой, добавляем заголовки
        string columns;
        if (!coloumnHash.get(table, columns)) {
            cerr << "Ошибка: не удалось получить заголовки для таблицы '" << table << "'!" << endl;
            lockTable(table, true);
            return;
        }
        outFile << columns << endl;
    }

    outFile << pkInt << ',' << values << '\n'; // Записываем строку с первичным ключом
    outFile.close();

    // Разблокируем таблицу
    lockTable(table, true);
    cout << "Команда выполнена!" << endl;
}

void BaseData::Insert(string& command) {
    string valuesPrefix = "VALUES";
    size_t pos = command.find_first_of(' ');

    if (pos == string::npos) {
        cout << "Ошибка! Неправильный синтаксис команды." << endl;
        return;
    }
    string table = command.substr(0, pos); // Извлечение имени таблицы
    command.erase(0, pos + 1);
    if (!tablesname.find(table)) {
        cout << "Ошибка! Таблица '" << table << "' не найдена." << endl;
        return;
    }
    if (command.substr(0, valuesPrefix.size()) != valuesPrefix) {
        cout << "Ошибка! Отсутствует ключевое слово VALUES." << endl;
        return;
    }
    command.erase(0, valuesPrefix.size() + 1); // Удаляем 'VALUES '
    if (command.front() != '(' || command.back() != ')') {
        cout << "Ошибка! Неправильный формат значений." << endl;
        return;
    }
    command.erase(0, 1); // Удаляем '('
    command.pop_back(); // Удаляем ')'

    string columnData;
    if (!coloumnHash.get(table, columnData)) {
        cerr << "Ошибка! Не удалось получить столбцы для таблицы '" << table << "'." << endl;
        return;
    }
    int columnCount = count(columnData.begin(), columnData.end(), ',') + 1;
    int valueCount = count(command.begin(), command.end(), ',') + 1;

    if (valueCount != columnCount - 1) { // -1 из-за первичного ключа
        cout << "Ошибка! Несоответствие количества значений и столбцов." << endl;
        return;
    }
    checkInsert(table, command);
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
void BaseData::isValidSelect(string& command) { 
    Filters conditions;
    Hash_table<string, Filters> cond; // Хэш-таблица для хранения условий

    if (command.find("FROM") != string::npos) {
        // Обработка столбцов
        while (command.substr(0, 4) != "FROM") {
            string token = command.substr(0, command.find_first_of(' '));
            if (token.find(',') != string::npos) token.pop_back(); // Удаляем запятую
            command.erase(0, command.find_first_of(' ') + 1);

            if (token.find('.') != string::npos) {
                token.replace(token.find('.'), 1, " ");
            } else {
                cout << "Ошибка, нарушен синтаксис команды!" << endl;
                return;
            }

            stringstream ss(token);
            ss >> conditions.table >> conditions.colona;

            // Проверка существования таблицы
            bool tableFound = false;
            for (int i = 0; i < tablesname.size(); ++i) { 
                if (tablesname.getElementAt(i) == conditions.table) {
                    tableFound = true;
                    break;
                }
            }

            if (!tableFound) { 
                cout << "Нет такой таблицы!" << endl;
                return;
            }

            string columns;
            if (!coloumnHash.get(conditions.table, columns)) { // Получаем столбцы из хэша
                cout << "Ошибка при получении столбцов!" << endl;
                return;
            }

            bool columnFound = false;
            stringstream columnStream(columns);
            while (getline(columnStream, token, ',')) { // Проверка существования столбца
                if (token == conditions.colona) {
                    columnFound = true;
                    break;
                }
            }

            if (!columnFound) {
                cout << "Нет такого столбца!" << endl;
                return;
            }

            cond.insert(conditions.table, conditions); // Добавляем условие в хэш-таблицу
        }

        command.erase(0, command.find_first_of(' ') + 1); // Скип FROM

        // Обработка таблиц
        int iter = 0;
        while (!command.empty()) {
            string token = command.substr(0, command.find_first_of(' '));
            if (token.find(',') != string::npos) token.pop_back();

            command.erase(0, command.find_first_of(' ') + 1);

            Filters currentFilter;
            if (!cond.get(token, currentFilter)) {
                cout << "Ошибка, указанные таблицы не совпадают или их больше!" << endl;
                return;
            }

            if (command.substr(0, 5) == "WHERE") break; // Останавливаемся при WHERE
            iter++;
        }

        if (command.empty()) {
            select(cond); // Выполняем select без условий WHERE
        } else {
            // Обработка условий WHERE
            command.erase(0, 6); // Убираем WHERE
            Hash_table<string, Filters> values; // Хэш-таблица для хранения условий фильтрации

            while (!command.empty()) {
                string token = command.substr(0, command.find_first_of(' '));
                command.erase(0, command.find_first_of(' ') + 1);

                if (token.find('.') != string::npos) {
                    token.replace(token.find('.'), 1, " ");
                    stringstream ss(token);
                    string table, column;
                    ss >> table >> column;

                    if (table != conditions.table) {
                        cout << "Ошибка, таблица в WHERE не совпадает с начальной!" << endl;
                        return;
                    }

                    conditions.colona = column;
                    conditions.check = true;

                    string valueToken = command.substr(0, command.find_first_of(' '));
                    command.erase(0, command.find_first_of(' ') + 1);
                    conditions.value = valueToken;

                    values.insert(column, conditions);
                } else {
                    cout << "Ошибка, нарушен синтаксис условия WHERE!" << endl;
                    return;
                }

                // Проверка логических операторов (AND/OR)
                if (!command.empty()) {
                    string logicOperator = command.substr(0, command.find_first_of(' '));
                    if (logicOperator == "AND" || logicOperator == "OR") {
                        conditions.logicOP = logicOperator;
                        command.erase(0, command.find_first_of(' ') + 1);
                    } else {
                        cout << "Ошибка, неверный логический оператор!" << endl;
                        return;
                    }
                }
            }

            selectWithLogic(cond, coloumnHash, coloumnHash, values); // Выполняем select с логикой
        }
    } else {
        cout << "Ошибка, нарушен синтаксис команды!" << endl;
    }
}

void BaseData::select(Hash_table<string, Filters>& filters) {
    // Проверка блокировки таблиц
    for (int i = 0; i < filters.size(); ++i) {
        Filters filter;
        filters.get(filters.getKeyAt(i), filter); // Получаем фильтр по индексу

        if (!checkLockTable(filter.table)) {
            cout << "Ошибка, таблица '" << filter.table << "' открыта другим пользователем!" << endl;
            return;
        }
    }
    for (int i = 0; i < filters.size(); ++i) { // Закрываем блокировки таблиц для выполнения операции
        Filters filter;
        filters.get(filters.getKeyAt(i), filter);

        string filepath = "../" + BD + '/' + filter.table + '/' + filter.table + "_lock.txt";
        filerec(filepath, "close");
    }
    // Получаем индексы столбцов и данные из таблиц
    Hash_table<string, int> stlbindex = findIndexStlb(filters); // Индексы столбцов
    Hash_table<string, string> tables = textInput(filters); // Данные из файлов
    sample(stlbindex, tables); // Выполняем выборку
    for (int i = 0; i < filters.size(); ++i) { // Открываем блокировки таблиц обратно после выполнения операции
        Filters filter;
        filters.get(filters.getKeyAt(i), filter);
        string filepath = "../" + BD + '/' + filter.table + '/' + filter.table + "_lock.txt";
        filerec(filepath, "open");
    }
}

void BaseData::selectWithValue(Hash_table<string, Filters>& filters, string table, Filters value) {
    // Проверка блокировок таблиц
    for (int i = 0; i < filters.size(); ++i) {
        Filters filter;
        filters.get(filters.getKeyAt(i), filter);
        if (!checkLockTable(filter.table)) {
            cout << "Ошибка, таблица открыта другим пользователем!" << endl;
            return;
        }
    }

    // Закрытие таблиц для операции
    string filepath;
    for (int i = 0; i < filters.size(); ++i) {
        Filters filter;
        filters.get(filters.getKeyAt(i), filter);
        filepath = "../" + BD + '/' + filter.table + '/' + filter.table + "_lock.txt";
        filerec(filepath, "close");
    }

    // Нахождение индексов столбцов
    Hash_table<string, int> stlbindex = findIndexStlb(filters);
    int stlbindexval = findIndexStlbCond(table, value.colona);
    int stlbindexvalnext = findIndexStlbCond(value.table, value.colona);

    // Загрузка данных из таблиц
    Hash_table<string, string> tables = textInput(filters);
    Hash_table<string, string> column = findTable(filters, tables, stlbindexvalnext, value.table);

    // Фильтрация строк
    for (int i = 0; i < filters.size(); ++i) {
        string tableName = filters.getKeyAt(i);
        string tableData;

        if (tables.get(tableName, tableData)) { // Получаем данные таблицы
            stringstream stream(tableData);
            string row, filetext;
            int iterator = 0;

            while (getline(stream, row)) {
                stringstream rowStream(row);
                string token;
                int currentIndex = 0;

                while (getline(rowStream, token, ',')) {
                    if (value.check) { // Простое условие
                        if (currentIndex == stlbindexval && token == value.value) {
                            filetext += row + '\n';
                            break;
                        }
                    } else { // Условие столбец
                        if (currentIndex == stlbindexval && token == column.getValueAt(iterator)) {
                            filetext += row + '\n';
                            break;
                        }
                    }
                    currentIndex++;
                }
                iterator++;
            }
            tables.insert(tableName, filetext); // Обновляем таблицу после фильтрации
        }
    }

    // Выборка данных
    sample(stlbindex, tables);

    // Открытие таблиц после завершения операции
    for (int i = 0; i < filters.size(); ++i) {
        Filters filter;
        filters.get(filters.getKeyAt(i), filter);
        filepath = "../" + BD + '/' + filter.table + '/' + filter.table + "_lock.txt";
        filerec(filepath, "open");
    }
}

void BaseData::selectWithLogic(Hash_table<string, Filters>& filter, Hash_table<string, string>& tables, Hash_table<string, string>& stolbec, Hash_table<string, Filters>& value) {
    // Проверка блокировки таблиц
    for (int i = 0; i < filter.size(); ++i) {
        Filters filters;
        filter.get(filter.getKeyAt(i), filters);
        bool check = checkLockTable(filters.table);
        if (!check) {
            cout << "Ошибка, таблица открыта другим пользователем!" << endl;
            return;
        }
    }
    // Блокировка таблиц
    for (int i = 0; i < filter.size(); ++i) {
        Filters condition;
        filter.get(filter.getKeyAt(i), condition);
        string filepath = "../" + BD + '/' + condition.table + '/' + condition.table + "_lock.txt";
        filerec(filepath, "close");
    }
    Hash_table<string, int> stlbindex = findIndexStlb(filter); // Нахождение индексов столбцов для select и условий
    Hash_table<string, int> stlbindexval;
    for (int i = 0; i < stolbec.size(); ++i) {
        string tableName = stolbec.getKeyAt(i);
        string stolbecName = stolbec.getValueAt(i);
        int index = findIndexStlbCond(tableName, stolbecName);
        stlbindexval.insert(stolbecName, index);
    }
    Hash_table<string, int> stlbindexvalnext;
    for (int i = 0; i < value.size(); ++i) {
        Filters val;
        value.get(value.getKeyAt(i), val);
        int index = findIndexStlbCond(val.table, val.colona);
        stlbindexvalnext.insert(val.colona, index);
    }
    Hash_table<string, string> tablesData = textInput(filter); // Чтение данных таблиц

    // Фильтрация данных
    for (int i = 0; i < filter.size(); ++i) {
        Filters condition;
        filter.get(filter.getKeyAt(i), condition);
        string tableData;
        if (tablesData.get(condition.table, tableData)) {
            stringstream stream(tableData);
            string row;
            string filteredData;
            int iterator = 0;
            while (getline(stream, row)) {
                Hash_table<string, bool> checkstr;
                for (int j = 0; j < value.size(); ++j) {
                    Filters val;
                    value.get(value.getKeyAt(j), val);
                    stringstream rowStream(row);
                    string token;
                    int currentIndex = 0;
                    bool check = false;

                    while (getline(rowStream, token, ',')) {
                        if (val.check) { // Простое условие
                            int targetIndex;
                            stlbindexval.get(val.colona, targetIndex);
                            if (currentIndex == targetIndex && token == val.value) {
                                check = true;
                                break;
                            }
                        } else { // Условие столбец
                            int targetIndex;
                            stlbindexval.get(val.colona, targetIndex);
                            string columnValue;
                            tablesData.get(val.table, columnValue);

                            if (currentIndex == targetIndex && token == columnValue) {
                                check = true;
                                break;
                            }
                        }
                        currentIndex++;
                    }
                    checkstr.insert(val.colona, check);
                }

                bool finalCheck = true;
                for (int j = 0; j < value.size(); ++j) {
                    Filters val;
                    value.get(value.getKeyAt(j), val);

                    bool currentCheck;
                    checkstr.get(val.colona, currentCheck);

                    if (val.logicOP == "and") {
                        finalCheck &= currentCheck;
                    } else {
                        finalCheck |= currentCheck;
                    }
                }

                if (finalCheck) {
                    filteredData += row + "\n";
                }
                iterator++;
            }
            tablesData.insert(condition.table, filteredData);
        }
    }
    sample(stlbindex, tablesData); // Выполнение выборки
    // Разблокировка таблиц
    for (int i = 0; i < filter.size(); ++i) {
        Filters condition;
        filter.get(filter.getKeyAt(i), condition);
        string filepath = "../" + BD + '/' + condition.table + '/' + condition.table + "_lock.txt";
        filerec(filepath, "open");
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

Hash_table<string, string> BaseData::findTable(Hash_table<string, Filters>& filters, Hash_table<string, string>& tablesHash, int stlbindexvalnext, string table) {
    Hash_table<string, string> columnHash;

    for (int i = 0; i < filters.size(); ++i) {
        Filters filter;
        filters.get(filters.getKeyAt(i), filter); // Получаем фильтр по индексу

        if (filter.table == table) {
            string tableData;
            if (tablesHash.get(table, tableData)) { // Получаем данные таблицы
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

void BaseData::sample(Hash_table<string, int>& stlbindex, Hash_table<string, string>& tables) {
    for (int i = 0; i < tables.size(); ++i) {
        string tableName = tables.getKeyAt(i);
        string onefile;
        if (!tables.get(tableName, onefile)) {
            cerr << "Ошибка: таблица с именем " << tableName << " не найдена." << endl;
            continue;
        }
        stringstream onefileStream(onefile);
        string row;
        while (getline(onefileStream, row)) {
            string needstlb;
            stringstream lineStream(row);
            string token;
            int currentIndex = 0;
            while (getline(lineStream, token, ',')) {
                int columnIndex = 0;
                if (!stlbindex.get(tableName, columnIndex)) {
                    cerr << "Ошибка: индекс столбца для таблицы " << tableName << " не найден." << endl;
                    break;
                }
                if (currentIndex == columnIndex) {
                    needstlb = token;
                    break;
                }
                currentIndex++;
            }
            if (i + 1 < tables.size()) { // Следующая таблица
                string nextTableName = tables.getKeyAt(i + 1);
                string twofile;
                if (!tables.get(nextTableName, twofile)) {
                    cerr << "Ошибка: таблица с именем " << nextTableName << " не найдена." << endl;
                    continue;
                }
                stringstream twofileStream(twofile);
                while (getline(twofileStream, row)) {
                    stringstream lineTwoStream(row);
                    string token;
                    currentIndex = 0;

                    while (getline(lineTwoStream, token, ',')) {
                        int columnIndex = 0;
                        if (!stlbindex.get(nextTableName, columnIndex)) {
                            cerr << "Ошибка: индекс столбца для таблицы " << nextTableName << " не найден." << endl;
                            break;
                        }
                        if (currentIndex == columnIndex) {
                            cout << needstlb << ' ' << token << endl;
                            break;
                        }
                        currentIndex++;
                    }
                }
            }
        }
    }
}