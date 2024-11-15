#ifndef ht_h
#define ht_h

#include "utility.h"

const size_t SIZE = 100; //Размер хэш-таблицы

struct HNode {
    string key;
    string value;
    HNode* next;

     // Конструктор для удобства создания узлов
    HNode(const string& k, const string& v) : key(k), value(v), next(nullptr) {}
};

struct Hash_table {
    HNode* table[SIZE];
    int sizetable = 0;

    Hash_table(); //Инициализация хэш таблицы
    ~Hash_table(); //Деконструктор

    int hashFunction(const string& key); // Хеш-функция
    void insert(string &key, string &value); //Функция долбавления элемента
    bool get(const string& key, string& value);
    bool remove(const string& key);
};

#endif // HT_H