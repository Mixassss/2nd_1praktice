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
        string logicOP; // Логический оператор (AND/OR)
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

    /// Вспомогательные функции ///
    bool checkLockTable(string table); // Функция проверки открытия таблицы
    void lockTable(string& table, bool open); // Функция закрытия таблицы
    void commands(string& command); // Функция ввода команд
};

#include "../src/commands.cpp"

#endif // COMMANDS_H