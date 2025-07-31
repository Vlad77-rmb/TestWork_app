import sqlite3
import sys
import random
import time
from datetime import datetime

class Worker:
    # инициализация 3х атрибутов класса worker
    def __init__(self, name, dob, sex):
        self.name = name
        self.dob = dob #date of birth
        self.sex = sex

    # преобразуем строку в объект datetime
    def get_age(self):
        birth = datetime.strptime(self.dob, "%Y-%m-%d")
        now = datetime.now()
        years = now.year - birth.year
        if (now.month, now.day) < (birth.month, birth.day):
            years -= 1
        return years

    # вставка данных в таблицу
    def store(self, db):
        db.execute("INSERT INTO workers VALUES (?, ?, ?)",
                   (self.name, self.dob, self.sex))

class StaffDB:
    # подключение к sqlite3
    def __init__(self):
        self.connection = sqlite3.connect("staff.db")
        self.cursor = self.connection.cursor()
        self.setup_db()

    # создание таблицы workers, если она не существует
    def setup_db(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                name TEXT,
                dob TEXT,
                sex TEXT
            )
        """)
        self.connection.commit()

    # добавление нового сотрудника
    def add_worker(self, worker):
        worker.store(self.cursor)
        self.connection.commit()

    # массовое добавление сотрудников в БД
    def add_many(self, workers):
        for w in workers:
            w.store(self.cursor)
        self.connection.commit()

    # Получение всех уникальных записей по параметру name
    def show_all_unique(self):
        self.cursor.execute("""
            SELECT DISTINCT name, dob, sex 
            FROM workers 
            ORDER BY name
        """)
        return self.cursor.fetchall()

    # Поиск мужчин с фамилией на букву F
    def find_male_f(self):
        self.cursor.execute("""
            SELECT name, dob, sex 
            FROM workers 
            WHERE sex = 'Male' AND name LIKE 'F%'
        """)
        return self.cursor.fetchall()

    # Оптимизация БД через создание составного индекса
    def make_faster(self):
        # Создание индекса в БД для ускорения поиска - первичный поиск по полу и после по имени
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS speed_search 
            ON workers(sex, name)
        """)
        self.connection.commit()

    # сжатие БД, освобождая неиспользуемое пространство
    def vacuum(self):
        self.cursor.execute("VACUUM")
        self.connection.commit()

    def close(self):
        self.connection.close()

    def clear_table(self):
        #Очистка таблицы перед новым заполнением
        self.cursor.execute("DELETE FROM workers")
        self.connection.commit()

# Формирование даты по заданным условиям
def random_date():
    year = random.randint(1960, 2000)
    month = random.randint(1, 12)

    # Високосный год, если делится на 4 и не делится на 100 или если делится на 400
    if month == 2:
        day = random.randint(1, 29 if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0) else 28)
    elif month in [4, 6, 9, 11]:
        day = random.randint(1, 30)
    else:
        day = random.randint(1, 31)

    return f"{year}-{month:02d}-{day:02d}"

def random_worker():
    first = random.choice(["James", "Robert", "John", "David", "Michael"])
    middle = random.choice(["Ivanovich", "Sergeevich", "Petrovich", "Anatolievich", "Dmitrievich"])
    last = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"])
    name = f"{last} {first} {middle}"
    dob = random_date()
    sex = random.choice(["Male", "Female"])

    return Worker(name, dob, sex)

def special_workers():
    workers = []
    names = ["Frank", "Fred", "Felix", "Ford"]
    middle = ["Ivanovich", "Sergeevich", "Petrovich", "Anatolievich", "Dmitrievich"]

    for _ in range(100):
        first = random.choice(names)
        last = "F" + random.choice(["ox", "itz", "rost", "ield"])
        middlename = random.choice(middle)
        name = f"{last} {first} {middlename}"
        dob = random_date()
        workers.append(Worker(name, dob, "Male"))

    return workers


def main():
    # Проверка количества аргументов командной строки
    if len(sys.argv) < 2:
        print("Используйте: program.py [режим] [аргументы]")
        return

    # Первый аргумент после имени скрипта
    mode = sys.argv[1]
    db = StaffDB()

    try:
        # режим очистки:
        if mode == "0":
            db.clear_table()
            db.vacuum()
            print("Таблица workers очищена и БД сжата")

        # создание таблицы StaffDB, подверждение успешного действия
        elif mode == "1":
            print("Готово")

        # Создание записи справочника сотрудников.
        elif mode == "2":
            if len(sys.argv) != 5:
                print("Нужно: program.py 2 'ФИО' 'ГГГГ-ММ-ДД' 'Пол'")
                return

            w = Worker(sys.argv[2], sys.argv[3], sys.argv[4])
            db.add_worker(w)
            print(f"Добавлено: {sys.argv[2]}, {sys.argv[3]}, {sys.argv[4]}")

        # Вывод всех строк справочника сотрудников
        elif mode == "3":
            print("Ожидание вывода списка сотрудников...")
            workers = db.show_all_unique()
            for w in workers:
                age = Worker(w[0], w[1], w[2]).get_age()
                print(f"{w[0]}, {w[1]}, {w[2]}, {age} лет")

        # Заполнение автоматически 1000000 строк справочника сотрудников
        elif mode == "4":
            db.clear_table()  # Очищаем таблицу перед заполнением!
            print("Создаю 1 млн записей...")
            many = [random_worker() for _ in range(1000000)]
            db.add_many(many)

            print("Добавляю 100 особых...")
            special = special_workers()
            db.add_many(special)

            print("Готово")

        # Результат выборки из таблицы по критерию: пол мужской, Фамилия начинается с "F".
        elif mode == "5":
            start = time.time()
            result = db.find_male_f()
            end = time.time()

            print(f"Найдено: {len(result)}")
            for w in result[:5]:
                print(f"{w[0]}, {w[1]}, {w[2]}")
            if len(result) > 5:
                print(f"...и еще {len(result) - 5}")

            print(f"Время: {end - start:.3f} сек")

        # Произвести оптимизацию базы данных или запросов к базе для ускорения выполнения пункта 5
        elif mode == "6":
            print("Тест без оптимизации:")
            start = time.time()
            result = db.find_male_f()
            end = time.time()
            print(f"Время: {end - start:.3f} сек")

            print("Оптимизирую...")
            db.make_faster()

            print("Тест с оптимизацией:")
            start = time.time()
            db.find_male_f()
            end = time.time()
            print(f"Время: {end - start:.3f} сек")
        else:
            print("Неизвестный режим")

    finally:
        db.close()


if __name__ == "__main__":
    main()