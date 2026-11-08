#!/usr/bin/env python3
# concurrency.pyw - emulating concurrency problems for a dbms 

import logging as log

log.basicConfig(level=log.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')

import os 
from dotenv import load_dotenv

import threading as th
import time

import sqlalchemy as sa
from sqlalchemy import text, select, update, exists
from sqlalchemy.dialects.mysql import insert

from sqlalchemy.orm import mapped_column, Mapped, declarative_base, Session, mapper
from sqlalchemy.dialects.mysql import YEAR

# for generating name data for insertions
import random as rd

# TODO: edit to make movie names
from faker import Faker
fake = Faker()

load_dotenv()

FILM_USER = os.getenv('FILM_USER')
FILM_PASSWORD = os.getenv('FILM_PASSWORD')
FILM_PORT = os.getenv('FILM_PORT')

CONN_CONF = f"mariadb+mariadbconnector://{FILM_USER}:{FILM_PASSWORD}@127.0.0.1:{FILM_PORT}/sakila"

# inflation rate of 6.1%
INFLATION = 6.1 / 100

engine = sa.create_engine(CONN_CONF, pool_pre_ping=True)

Base = declarative_base()

class film(Base):
    __tablename__ = 'film'
    film_id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    title: Mapped[str] = mapped_column(sa.String(128))
    release_year: Mapped[int] = mapped_column(YEAR)
    rental_rate: Mapped[float] = mapped_column(sa.Numeric)
    replacement_cost: Mapped[float] = mapped_column(sa.Numeric)


# script with threading implemented innit

def main():

    isolation_levels = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]

    total_time = -16.0

    for iso in isolation_levels:

        start = time.time()
        print(f"Starting {iso}: Dirty Read.")

        dirty_read(iso)

        time.sleep(2)

        print(f"Starting {iso}: Non Repeatable Read.")
        nr_read(iso)

        time.sleep(2)

        print(f"Starting {iso}: Phantom Read.")
        phantom_read(iso)

        end = time.time()
        time_elapsed = end - start

        print(f"Finished {iso}. Time Elapsed: {time_elapsed:.2}")
        total_time += time_elapsed

    time.sleep(2)
    print("\n\n=======================================================")

    print(f"Program Done. Total Time Elapsed: { total_time + 16 }.\n"
            f"Without pauses: {total_time:.2}")

    # TODO: Possible stress testing until I get an exception. 
    



def dirty_read(isolation_level:str):
    '''
    Dirty read..

    ifl somethigns wrongs with this but not sure.
    '''
    th1 = th.Thread(target=film_inflate_all, args=[isolation_level], kwargs={'with_commit':False})
    th2 = th.Thread(target=film_read, args=[isolation_level])

    th1.start()
    th2.start()

    th2.join()
    th1.join()



def nr_read(isolation_level:str):
    '''
    Non repeatable read 
    '''

    th1 = th.Thread(target=film_read, args=[isolation_level])
    th2 = th.Thread(target=film_inflate_all, args=[isolation_level], kwargs={'with_commit':True})
    th3 = th.Thread(target=film_read, args=[isolation_level])

    th1.start()
    th2.start()
    th3.start()

    th1.join()
    th2.join()
    th3.join()




def phantom_read(isolation_level:str):
    '''
    Phantom read
    '''

    th1 = th.Thread(target=film_read, args=[isolation_level])
    th2 = th.Thread(target=film_insert, args=[isolation_level], kwargs={'with_commit':True})
    th3 = th.Thread(target=film_read, args=[isolation_level])

    th1.start()
    th2.start()
    th3.start()

    th1.join()
    th2.join()
    th3.join()




# Standard Transactions 

def film_read(isolation_level:str):
    '''
    Simple query that fr just reads all the data.
    '''

    with Session(engine) as sess:

        sess.connection(execution_options={"isolation_level": isolation_level})

        query = select(film)

        query = sess.scalars(query).all()

        for i in query:

            print("++--------------------------------------------------------------------------++")
            print(f"| {i.title} |  {i.release_year} | {i.rental_rate} | {i.replacement_cost}  |")

        print("++--------------------------------------------------------------------------++\n")

        print("End Of Print...\n\n")




def film_inflate_all(isolation_level:str, with_commit:bool):
    '''
    Updates all entries in film by increasing replacement_cost by a constant
    inflation rate. 

    '''

    with Session(engine) as sess:

        sess.connection(execution_options={"isolation_level": isolation_level})

        query = update(film).values(
                replacement_cost = film.replacement_cost + film.replacement_cost * INFLATION
                )

        query = sess.execute(query)

        if with_commit:
            sess.commit()


def film_insert(isolation_level:str, with_commit:bool):
    '''
    inserts a new entry with random information into the film table.
    '''

    with Session(engine) as sess:

        sess.connection(execution_options={"isolation_level": isolation_level})

        # TODO: random movie name
        i_name = fake.unique.name()
        i_year = int(fake.date()[:4])
        i_rr = (rd.random() * 10)
        i_rc = (rd.random() * 100)

        i_film = film(title=i_name,
                      release_year=i_year,
                      rental_rate=i_rr,
                      replacement_cost=i_rc)

        sess.add(i_film)

        if with_commit:
            sess.commit()



main()
