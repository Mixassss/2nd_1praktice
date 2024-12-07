#include "../include/ht.h"

template <typename Key, typename Value>
Hash_table<Key, Value>::Hash_table() {
    for(size_t i = 0; i < SIZE; ++i) {
        table[i] = nullptr; //Присваиванпие каждой ноды, нулевого значения
    }
}

template <typename Key, typename Value>
Hash_table<Key, Value>::~Hash_table() {
    for (size_t i = 0; i < SIZE; ++i) {
        HNode<Key, Value>* current = table[i];
        while (current) {
            HNode<Key, Value>* toDelete = current;
            current = current->next; // Освобождение каждой ноды
            delete toDelete;
        }
    }
}

template <typename Key, typename Value>
int Hash_table<Key, Value>::hashFunction(const Key& key) {
    hash<Key> hashFn;
    return hashFn(key) % SIZE;
}

template <typename Key, typename Value>
void Hash_table<Key, Value>::insert(const Key& key, const Value& value) {
    int hashValue = hashFunction(key); // Хэш значение соответствующее этому ключу
    HNode<Key, Value>* newPair = new HNode(key, value); // Используем конструктор HNode

    if(table[hashValue] == nullptr) {
        table[hashValue] = newPair; // Если ячейка пуста, вставляем новый элемент
        sizetable++;
    } else {
        HNode<Key, Value>* current = table[hashValue];
        while(current) { 
            if(current->key == key) {
                current->value = value; // Обновляем значение, если ключ существует
                delete newPair; // Удаляем временный узел
                return;
            }
            if (current->next == nullptr) break; // Если достигли конца цепочки
            current = current->next;
        }
        current->next = newPair; // Добавляем новый элемент в конец цепочки
        sizetable++;
    }
}

template <typename Key, typename Value>
bool Hash_table<Key, Value>::get(const Key& key, Value& value) {
    int HashValue = hashFunction(key);
    HNode<Key, Value>* current = table[HashValue];
    while(current) {
        if(current->key == key) {
            value = current->value;
            return true; // Ключ найден
        }
        current = current->next;
    }
    return false; // Ключ не найден
}

template <typename Key, typename Value>
bool Hash_table<Key, Value>::remove(const Key& key) {
    int HashValue = hashFunction(key);
    HNode<Key, Value>* current = table[HashValue];
    HNode<Key, Value>* perv = nullptr;

    while(current) {
        if(current->key == key) {
            if(perv) {
                perv->next = current->next; //Удаление из цепочки
            } else {
                table[HashValue] = current->next; // Если это первый элемент в цепочке
            }
            delete current; // Освобождаем память
            return true; // Успешное удаление
        }
        perv = current;
        current = current->next;
    }
    return false; // Ключ не найден
}

template <typename Key, typename Value>
int Hash_table<Key, Value>::size() const {
    return sizetable;
}