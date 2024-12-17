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
        string table;
        string logicOP; // Логический оператор (AND/OR)
        bool check;
    };

    void parser();
    void createdirect();
    void createLockFile(const string& dir, const string& tableName);
    void createPKFile(const string& dir, const string& tableName);

    void checkInsert(string& table, string& values); // Проверка ввода команды инсерта
    void Insert(string& command); // Функция инсерта
    void delAll(string& table); // Функция очистки всей таблицы
    void deleteZnach(string& table, string& stolbec, string& values); // Функция удаления строк по значению
    void deleteFilter(Hash_table<string, Filters>& filter, string& table); //Функция удаления по условию
    void Delete(string& command);
    bool isColumnValid(const string& columnString, const string& column);
    bool processLogicalOperator(string& conditions, Hash_table<string, Filters>& yslov, const string& table);
    void select(Hash_table<string, Filters>& filter); // Функция команды select
    void selectWithValue(SinglyLinkedList<Filters>& filters, string& table, string& column, Filters value);
    void selectWithLogic(SinglyLinkedList<Filters>& conditions, SinglyLinkedList<string>& table, SinglyLinkedList<string>& stolbec, SinglyLinkedList<Filters>& value);

    /// Вспомогательные функции ///
    bool checkLockTable(string table); // Функция проверки открытия таблицы
    void lockTable(string& table, bool open); // Функция закрытия таблицы
    void commands(string& command); // Функция ввода команд
    void sample(Hash_table<int, string>& stlbindex, Hash_table<int, string>& tables); // Функция выбора
    Hash_table<string, int> findIndexStlb(Hash_table<string, Filters>& filters);
    int findIndexStlbCond(string table, string stolbec);
    Hash_table<string, string> findTable(SinglyLinkedList<Filters>& filters, Hash_table<string, string>& tablesHash, int stlbindexvalnext, string table);
    Hash_table<string, string> textInput(Hash_table<string, Filters>& filters); // Функция инпута текста из таблиц
};

#include "../src/commands.cpp"

#endif // COMMANDS_H