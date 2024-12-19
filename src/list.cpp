#include "../include/list.h"

template <typename T>
Node<T>::Node(T value) {
    data = value;
    next = nullptr;
} 

template <typename T>
SinglyLinkedList<T>::SinglyLinkedList() {
    head = nullptr; //Если список не содержит элементов, то присваиваем нулевое значение
}

template <typename T>
SinglyLinkedList<T>::~SinglyLinkedList() {
    while(!isEmpty()) {
        popFront(); //Вызывает список, после которого очищает память
    }
}

template <typename T>
bool SinglyLinkedList<T>::isEmpty() const {
    return elementCount == 0;
}

template <typename T>
void SinglyLinkedList<T>::print() {
    Node<T>* current = head;
    while (current) {
        cout << current->data << " ";
        current = current->next;
    }
    cout << endl;
}

template <typename T>
void SinglyLinkedList<T>::pushFront(T value) { 
    Node<T>* newNode = new Node(value);
    newNode->next = head; //Следующий узел, становится текущей головой списка
    head = newNode;
    elementCount++;
}

template <typename T>
void SinglyLinkedList<T>::pushBack(T value) {
    Node<T>* newNode = new Node(value);
    if(head == nullptr) { //Условие если список пуст
        head = newNode;
    } else {
        Node<T>* current = head;
        while(current->next != nullptr) {
            current = current->next; //Присваиваем current следующее значение
        }
        current->next = newNode;
    }
    elementCount++;
}

template <typename T>
void SinglyLinkedList<T>::popFront() {
    if(head == nullptr) return;
    Node<T>* nextHead = head->next; //Создание временной переменной, для сохранения
    delete head;
    head = nextHead;
    elementCount--;
}

template <typename T>
void SinglyLinkedList<T>::popBack() {
    if (head == nullptr) return; // Если список пуст
    if (head->next == nullptr) { // Если один элемент
        delete head;
        head = nullptr; // Исправлено
    } else {
        Node<T>* current = head;
        while (current->next->next != nullptr) {
            current = current->next;
        }
            delete current->next; // Удаляем последний элемент
            current->next = nullptr; // Обнуляем указатель на следующий элемент
        }
    elementCount--;
}

template <typename T>
void SinglyLinkedList<T>::removeAt(T value) {
    if (isEmpty()) return;
    if (head->data == value) {
        popFront();
        return;
    }
    Node<T>* current = head;
    while (current->next != nullptr && current->next->data != value) {
        current = current->next;
    }
    if (current->next != nullptr) {
        Node<T>* nodeToDelete = current->next;
        current->next = nodeToDelete->next;
        delete nodeToDelete;
        elementCount--;
    }
}

template <typename T>
bool SinglyLinkedList<T>::find(T value) {
    Node<T>* current = head;
    while (current != nullptr) {
        if (current->data == value) {
            return true;
        }
        current = current->next;
    }
    return false;
}

template <typename T>
void SinglyLinkedList<T>::clearSList() {
    while (!isEmpty()) {
        popFront();
    }
}

template <typename T>
T SinglyLinkedList<T>::getElementAt(int index) const {
    if (index < 0 || index >= elementCount) {
        throw out_of_range("Index out of range");
    }

    Node<T>* current = head;
    for (int i = 0; i < index; i++) {
        current = current->next;
    }

    return current->data;
}

template <typename T>
int SinglyLinkedList<T>::getIndex(T value) {
    Node<T>* current = head;
    int index = 0;
    while (current) {
        if (current->data == value) {
            return index; // Возвращаем индекс, если значение найдено
        }
        current = current->next;
        index++;
    }
    return -1; // Возвращаем -1, если значение не найдено
}

template<typename T>
void SinglyLinkedList<T>::replace(int index, T newValue) {
    if (index < 0 || index >= size()) {
        cout << "Index out of bounds." << endl;
        return;
    }

    Node<T>* current = head;
    for (int i = 0; i < index; i++) {
        current = current->next;
    }
    current->data = newValue;
}

template <typename T>
Node<T>* SinglyLinkedList<T>::getHead() const {
    return head;
}

template <typename T>
size_t SinglyLinkedList<T>::size() {
    return elementCount;
}