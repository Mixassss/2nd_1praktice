# Практика 1: Разработка СУБД
Задание
Требуется реализовать реляционную СУБД с поддержкой запросов на языке SQL. СУБД
использует единственный тип данных - строка.
Требуется поддержать следующие SQL выражения:
  1. SELECT <> FROM <> - выборка
  2. WHERE и операторы OR , AND - фильтрация
  3. INSERT INTO - вставка данных в таблицы
  4. DELETE FROM - удаление данных из таблицы

    INSERT INTO aircraftInfo VALUES ('213','Boeing', '1000')
    INSERT INTO aircraftInfo VALUES ('212','Boeing', '500')
    INSERT INTO aircraftInfo VALUES ('212','Boeing', '500')
    INSERT INTO aircraftInfo VALUES ('3534598','Airbus', '10000')
    INSERT INTO aircraftInfo VALUES ('3584598','Airbus', '10000')
    INSERT INTO departureInfo VALUES ('1234','Moscow','5','Novosibirsk')
    INSERT INTO departureInfo VALUES ('1234','Kyzyl','5','Dushanbe')
    INSERT INTO departureInfo VALUES ('2889','Samara','3','Novosibirsk')

    DELETE FROM aircraftInfo WHERE model = 'Boeing'
    DELETE FROM aircraftInfo WHERE model = 'Airbus'
    DELETE FROM aircraftInfo WHERE number = '212' AND model = 'Boeing'
    DELETE FROM aircraftInfo WHERE number = '212' OR model = 'Boeing'
    DELETE FROM aircraftInfo WHERE number = '3534598' AND model = 'Airbus'

    SELECT aircraftInfo.number, aircraftInfo.model, aircraftInfo.price FROM aircraftInfo WHERE aircraftInfo.model = 'Boeing'
    SELECT aircraftInfo.number, aircraftInfo.model FROM aircraftInfo
    SELECT departureInfo.city_depart, departureInfo.city_arrival FROM departureInfo WHERE departureInfo.city_arrival = 'Novosibirsk' AND departureInfo.gate = '5'

    
