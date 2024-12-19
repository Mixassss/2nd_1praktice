#ifndef ht_h
#define ht_h

#include "utility.h"

const size_t SIZE = 100; //Размер хэш-таблицы

template <typename Key, typename Value>
struct HNode {
    Key key;
    Value value;
    HNode* next;

     // Конструктор для удобства создания узлов
    HNode(const Key& k, const Value& v) : key(k), value(v), next(nullptr) {}
};

template <typename Key, typename Value>
struct Hash_table {
    HNode<Key, Value>* table[SIZE];
    int sizetable = 0;

    Hash_table(); //Инициализация хэш таблицы
    ~Hash_table(); //Деконструктор

    int hashFunction(const Key& key); // Хеш-функция
    void insert(const Key& key, const Value& value); //Функция долбавления элемента
    bool get(const Key& key, Value& value);
    bool remove(const Key& key);
    int size() const;
    Key getKeyAt(int index) const;
    Value getValueAt(int index) const ;
    bool containsKey(const Key& key);
};

#include "../src/hash_table.cpp"

#endif // HT_H