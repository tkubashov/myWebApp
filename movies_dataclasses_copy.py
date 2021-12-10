import datetime
import uuid
from dataclasses import dataclass, field


@dataclass
class Genre:
    name: str
    description: str
    created_at: datetime.datetime = datetime.datetime.now()
    updated_at: datetime.datetime = datetime.datetime.now()
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class FilmWork:
    title: str
    description: str
    creation_date: datetime.datetime
    certificate: str
    file_path: str
    type: str
    rating: float = field(default=0.0)
    created_at: datetime.datetime = datetime.datetime.now()
    updated_at: datetime.datetime = datetime.datetime.now()
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    birth_date: datetime.datetime
    created_at: datetime.datetime = datetime.datetime.now()
    updated_at: datetime.datetime = datetime.datetime.now()
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmwork:
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created_at: datetime.datetime = datetime.datetime.now()
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmwork:
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created_at: datetime.datetime = datetime.datetime.now()
    id: uuid.UUID = field(default_factory=uuid.uuid4)


def sqlrow_to_dataclass(table, row):
    if table == 'film_work':
        return FilmWork(
                title=row['title'],
                description=row['description'],
                creation_date=row['creation_date'],
                certificate=row['certificate'],
                file_path=row['file_path'],
                type=row['type'],
                rating=row['rating'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                id=row['id'],
            )
    elif table == 'person':
        return Person(
            full_name=row['full_name'],
            birth_date=row['birth_date'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            id=row['id'],
        )
    elif table == 'genre':
        return Genre(
            name=row['name'],
            description=row['description'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            id=row['id'],
        )
    elif table == 'person_film_work':
        return PersonFilmwork(
                film_work_id=row['film_work_id'],
                person_id=row['person_id'],
                role=row['role'],
                created_at=row['created_at'],
                id=row['id'],
            )
    elif table == 'genre_film_work':
        return GenreFilmwork(
                film_work_id=row['film_work_id'],
                genre_id=row['genre_id'],
                created_at=row['created_at'],
                id=row['id'],
            )