from sqlalchemy import func, desc, and_, distinct, select

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    """1Знайти 5 студентів із найбільшим середнім балом з усіх предметів
    """

    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2():
    """Знайти студента із найвищим середнім балом з певного предмета..
    """

    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == 3).group_by(Student.id).order_by(desc('avg_grade')).limit(1).all()
    return result


def select_3():
    """3Знайти середній бал у групах з певного предмета.
        """

    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .join(Discipline) \
        .filter(Discipline.id == 3).group_by(Group.id).limit(4).all()
    return result


def select_4():
    """4Знайти середній бал на потоці (по всій таблиці оцінок)..
    """

    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .join(Discipline) \
        .group_by(Group.id).limit(4).all()
    return result


def select_5():
    """5Знайти, які курси читає певний викладач.
    """

    result = session.query(Discipline.name) \
        .select_from(Discipline) \
        .join(Teacher) \
        .filter(Teacher.id == 3).all()
    return result


def select_6():
    """6Знайти список студентів у певній групі.
        """

    result = session.query(Student.fullname) \
        .select_from(Student) \
        .join(Group) \
        .filter(Group.id == 2).all()
    return result


def select_7():
    """7Знайти оцінки студентів в окремій групі з певного предмета.
    """

    result = session.query(Student.fullname, Grade.grade) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Student) \
        .join(Group) \
        .filter(and_(Discipline.id == 4, Group.id == 2)).all()
    return result


def select_8():
    """8Знайти середній бал, який ставить певний викладач зі своїх предметів."""
    result = session.query(distinct(Teacher.fullname), func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Teacher) \
        .filter(Teacher.id == 2).group_by(Teacher.fullname).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_9():
    """9Знайти список курсів, які відвідує певний студент.
    """
    result = session.query(Discipline.name) \
        .select_from(Discipline) \
        .join(Grade) \
        .join(Student) \
        .filter(Student.id == 20).group_by(Discipline.name).all()
    return result


def select_10():
    """10Список курсів, які певному студенту читає певний викладач.
    """
    result = session.query(Discipline.name) \
        .select_from(Discipline) \
        .join(Teacher) \
        .join(Grade) \
        .join(Student) \
        .filter(and_(Student.id == 25, Teacher.id == 1)).group_by(Discipline.name).all()
    return result


if __name__ == "__main__":
    print(select_1())
    print(select_2())
    print(select_3())
    print(select_4())
    print(select_5())
    print(select_6())
    print(select_7())
    print(select_8())
    print(select_9())
    print(select_10())
