#ifndef list_h
#define list_h

#include "utility.h"

template <typename T>
struct Node {
    T data;
    Node* next; //Указатель на след. элемент
    
    Node(T value); //Конструктор узла
};

template <typename T>
struct SinglyLinkedList {
    Node<T>* head;
    size_t elementCount = 0;

    SinglyLinkedList(); //Конструктор
    ~SinglyLinkedList(); //Деконструктор

    bool isEmpty() const;
    void print(); // ф-ия вывода списка
    void pushFront(T value); //Добавление в начало списка
    void pushBack(T value); //Добавление в конец списка
    void popFront(); //Удаление в начале списка
    void popBack(); //Удаление в конце списка
    void removeAt(T value); //Удаление по индексу
    bool find(T value); //Поиск значений в списке
    void clearSList();
    T getElementAt(int index) const;
    int getIndex(T value);
    void replace(int index, T newValue);
    Node<T>* getHead() const;
    size_t size();
};

#include "../src/list.cpp"

#endif // LIST_H