from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import *
from random import randint

# connect to sqlite database
engine = create_engine('sqlite:///demo.db')

# define schema
Base = declarative_base()

class Users(Base):
    __tablename__ = "users"
    UserId = Column(Integer, primary_key=True)
    Title = Column(String)
    FirstName = Column(String)
    LastName = Column(String)
    Email = Column(String)
    Username = Column(String)
    DOB = Column(DateTime)

class Uploads(Base):
    __tablename__ = "uploads"
    UploadId = Column(Integer, primary_key=True)
    UserId = Column(Integer)
    Title = Column(String)
    Body = Column(String)
    Timestamp = Column(DateTime)

# create tables
Users.__table__.create(bind=engine, checkfirst=True)
Uploads.__table__.create(bind=engine, checkfirst=True)

# extract simulated data
import requests

url = 'https://randomuser.me/api/?results=10'
users_json = requests.get(url).json()

url2 = 'https://jsonplaceholder.typicode.com/posts/'
uploads_json = requests.get(url2).json()

# transform data, ready for loading stage
from datetime import datetime, timedelta

users, uploads = [], []

for i, result in enumerate(users_json['results']):
    row = {}
    row['UserId'] = i
    row['Title'] = result['name']['title']
    row['FirstName'] = result['name']['first']
    row['LastName'] = result['name']['last']
    row['Email'] = result['email']
    row['Username'] = result['login']['username']
    dob = datetime.strptime(result['dob']['date'],'%Y-%m-%dT%H:%M:%SZ')
    row['DOB'] = dob.date()
    users.append(row)

for result in uploads_json:
    row = {}
    row['UploadId'] = result['id']
    row['UserId'] = result['userId']
    row['Title'] = result['title']
    row['Body'] = result['body']
    delta = timedelta(seconds=randint(1,86400))
    row['Timestamp'] = datetime.now() - delta
    uploads.append(row)

# create new Session and commit to database
Session = sessionmaker(bind=engine)
session = Session()

for user in users:
    row = Users(**user)
    session.add(row)

for upload in uploads:
    row = Uploads(**upload)
    session.add(row)

session.commit()

# Aggregations
# define new table
class UploadCounts(Base):
    __tablename__ = "upload_counts"
    UserId = Column(Integer, primary_key=True)
    LastActive = Column(DateTime)
    PostCount = Column(Integer)

# create table
UploadCounts.__table__.create(bind=engine, checkfirst=True)

# connect to database and execute query
connection = engine.connect()

query = select([Uploads.UserId,
    func.max(Uploads.Timestamp).label('LastActive'),
    func.count(Uploads.UploadId).label('PostCount')]).group_by('UserId')

results = connection.execute(query)

# loop through results and commit to aggregates table
for result in results:  
    row = UploadCounts(**result)
    session.add(row)

session.commit()
session.close()