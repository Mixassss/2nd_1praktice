#ifndef commands_h
#define commands_h

#include "utility.h"
#include "ht.h"
#include "list.h"
#include "json.hpp"

int countingLine (string& fin);
string fileread (string& filename); // Производим чтение из файла
void filerec (string& filename, string data); // Производим запись в файл

struct BaseData {
    string BD; // название БД
    int rowLimits; // лимит строк
    SinglyLinkedList<string> nametables; // названия таблиц
    SinglyLinkedList<string> stlb; // столбцы таблиц
    SinglyLinkedList<int> fileindex; // кол-во файлов таблиц

    struct Filter { // структура для фильтрации
        string table;
        string column;
        string value;
        string logicOP;
        bool check; 
    };

    void parser();
    void createdirect();

    /// Функии для INSERT ///
    void insert(string& table, string& values); // Проверка ввода команды инсерта
    void isValidInsert(string& command); // Функция инсерта

    /// Функции для DELETE ///
    void delAll(string& table); // Функция очистки всей таблицы
    void delWithValue(string& table, string& stolbec, string& values); // Функция удаления строк по значению
    void delWithLogic(SinglyLinkedList<Filter>& conditions, string& table); //Функция удаления по условию
    void isValidDel(string& command); // Поверка синтаксиса команды 

    /// Функции для SELECT ///
    void isValidSelect(string& command);
    void select(SinglyLinkedList<Filter>& conditions); // Функция команды select
    void selectWithValue(SinglyLinkedList<Filter>& conditions, string& table, string& stolbec, struct Filter value);
    void selectWithLogic(SinglyLinkedList<Filter>& conditions, SinglyLinkedList<string>& table, SinglyLinkedList<string>& stolbec, SinglyLinkedList<Filter>& value);

    /// Вспомогательные функции ///
    bool checkLockTable(string table); // Функция проверки открытия таблицы
    void checkcommand(string& command); // Функция ввода команд
    void sample(SinglyLinkedList<int>& stlbindex, SinglyLinkedList<string>& tables); // Функция выбора
    SinglyLinkedList<int> findIndexStlb(SinglyLinkedList<Filter>& conditions);
    int findIndexStlbCond(string table, string stolbec);
    SinglyLinkedList<string> findStlbTable(SinglyLinkedList<Filter>& conditions, SinglyLinkedList<string>& tables, int stlbindexvalnext, string table);
    SinglyLinkedList<string> textInFile(SinglyLinkedList<Filter>& conditions); // Функция инпута текста из таблиц
};

#include "../src/commands.cpp"

#endif // COMMANDS_H