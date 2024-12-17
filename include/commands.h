#ifndef commands_h
#define commands_h

#include "utility.h"
#include "ht.h"
#include "list.h"
#include "json.hpp"

int countingLine (const string& filename);
string fileread (const string& filename); // Производим чтение из файла
void filerec (const string& filename, const string& data); // Производим запись в файл

struct BaseData {
    string BD; // Название БД
    int rowLimits; // Лимит строк
    SinglyLinkedList<string> tablesname; // Список названий таблиц
    Hash_table<string, string> coloumnHash; // Хэш таблица для работы со столбцами
    Hash_table<string, int> fileCountHash; // Хэш таблица для количества файлов таблиц

    struct Filters { // Структура для фильтрации
        string colona; // Имя столбца
        string value; // Значение для сравнения
        string table; // Таблица
        string logicOP; // Логический оператор (AND/OR)
        bool check;
    };

    void parser();
    void createdirect();
    void createLockFile(const string& dir, const string& tableName);
    void createPKFile(const string& dir, const string& tableName);

    /// Функии для INSERT ///
    void checkInsert(string& table, string& values); // Проверка ввода команды инсерта
    void Insert(string& command); // Функция инсерта

    /// Функции для DELETE ///
    void delAll(string& table); // Функция очистки всей таблицы
    void deleteZnach(string& table, string& stolbec, string& values); // Функция удаления строк по значению
    void deleteFilter(Hash_table<string, Filters>& filter, string& table); //Функция удаления по условию
    void Delete(string& command); // Поверка синтаксиса команды 
    bool isColumnValid(const string& columnString, const string& column);
    bool processLogicalOperator(string& conditions, Hash_table<string, Filters>& yslov, const string& table);

    /// Функции для SELECT ///
    void isValidSelect(string& command);
    void select(Hash_table<string, Filters>& filter); // Функция команды select
    void selectWithValue(Hash_table<string, Filters>& filters, string table, Filters value);
    void selectWithLogic(Hash_table<string, Filters>& conditions, Hash_table<string, string>& tables, Hash_table<string, string>& stolbec, Hash_table<string, Filters>& value);

    /// Вспомогательные функции ///
    bool checkLockTable(string table); // Функция проверки открытия таблицы
    void lockTable(string& table, bool open); // Функция закрытия таблицы
    void commands(string& command); // Функция ввода команд
    void sample(Hash_table<string, int>& stlbindex, Hash_table<string, string>& tables); // Функция выбора
    Hash_table<string, int> findIndexStlb(Hash_table<string, Filters>& filters);
    int findIndexStlbCond(string table, string stolbec);
    Hash_table<string, string> findTable(Hash_table<string, Filters>& filters, Hash_table<string, string>& tablesHash, int stlbindexvalnext, string table);
    Hash_table<string, string> textInput(Hash_table<string, Filters>& filters); // Функция инпута текста из таблиц
};

#include "../src/commands.cpp"

#endif // COMMANDS_H