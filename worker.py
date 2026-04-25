from rq import Connection, Worker

from app.database import Base, engine
from app.queue import redis_conn

if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)
    with Connection(redis_conn):
        worker = Worker(["csv_jobs"])
        worker.work()
