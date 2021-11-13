import psycopg2
import sqlalchemy as sa
import datetime
from random import choice, randint, uniform
from tqdm import tqdm

#Establishing the connection
with psycopg2.connect(database="postgres",
					  user='postgres', 
					  password='123', 
					  host='127.0.0.1', 
					  port= '5432') as conn:
	with conn.cursor() as cursor:
		# Create schema
		sql_schema = '''CREATE SCHEMA IF NOT EXISTS airflow'''

		cursor.execute(sql_schema)

		# conn.commit()

		#Creating table users
		sql_users ='''CREATE TABLE IF NOT EXISTS airflow.users(
		   ID_USER BIGSERIAL primary key,
		   ID_PRODUCTS INT,
		   NAME CHAR(64) NOT NULL,
		   AGE INT,
		   SEX CHAR(1),
		   INCOME FLOAT,
		   DATE_ADDED timestamp default NULL
		)'''
		cursor.execute(sql_users)

		# Creating table products
		sql_products='''CREATE TABLE IF NOT EXISTS airflow.products(
		   ID_PRODUCTS INT,
		   NAME_PRODUCT CHAR(64) NOT NULL,
		   DATE_ADDED timestamp default NULL
		)
		'''
		cursor.execute(sql_products)

		# Insert in table products
		insert_product = (
			'''INSERT INTO airflow.products (ID_PRODUCTS, NAME_PRODUCT, DATE_ADDED)
			VALUES (%s, %s, %s)'''
		)
		data = [(1, 'books', datetime.datetime.fromtimestamp(randint(1234567891, 1634567891))),
				(2, 'films', datetime.datetime.fromtimestamp(randint(1234567891, 1634567891))),
				(3, 'magazine', datetime.datetime.fromtimestamp(randint(1234567891, 1634567891))),
				(4, 'games', datetime.datetime.fromtimestamp(randint(1234567891, 1634567891))),
				(5, 'manga', datetime.datetime.fromtimestamp(randint(1234567891, 1634567891))),
				(6, 'newspaper', datetime.datetime.fromtimestamp(randint(1234567891, 1634567891))),
				(7, 'file', datetime.datetime.fromtimestamp(randint(1234567891, 1634567891))),
				(8, 'yellow pages', datetime.datetime.fromtimestamp(randint(1234567891, 1634567891))),]
		cursor.executemany(insert_product, data)

		# Insert in table users
		names = ['James', 'Robert', 'John', 'Michael', 'William', 'David', 'Richard', 'Joseph', 
		'Thomas', 'Charles', 'Christopher', 'Daniel', 'Matthew', 'Anthony', 'Mark', 'Kayla',
		'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kenneth', 'Kevin', 'Brian', 'George', 
		'Edward', 'Ronald', 'Timothy', 'Jason', 'Jeffrey', 'Ryan', 'Jacob', 'Gary', 'Nicholas', 
		'Eric', 'Jonathan', 'Stephen', 'Larry', 'Justin', 'Scott', 'Brandon', 'Benjamin', 'Samuel', 
		'Gregory', 'Frank', 'Alexander', 'Raymond', 'Patrick', 'Jack', 'Dennis', 'Jerry', 'Tyler', 
		'Aaron', 'Jose', 'Adam', 'Henry', 'Nathan', 'Douglas', 'Zachary', 'Peter', 'Kyle', 'Walter', 
		'Ethan', 'Jeremy', 'Harold', 'Keith', 'Christian', 'Roger', 'Noah', 'Gerald', 'Carl', 'Terry', 
		'Sean', 'Austin', 'Arthur', 'Lawrence', 'Jesse', 'Dylan', 'Bryan', 'Joe', 'Jordan', 'Billy', 
		'Bruce', 'Albert', 'Willie', 'Gabriel', 'Logan', 'Alan', 'Juan', 'Wayne', 'Roy', 'Ralph', 
		'Randy', 'Eugene', 'Vincent', 'Russell', 'Elijah', 'Louis', 'Bobby', 'Philip', 'Johnny', 
		'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 
		'Sarah', 'Karen', 'Nancy', 'Lisa', 'Betty', 'Margaret', 'Sandra', 'Ashley', 'Kimberly', 
		'Emily', 'Donna', 'Michelle', 'Dorothy', 'Carol', 'Amanda', 'Melissa', 'Deborah', 'Stephanie', 
		'Rebecca', 'Sharon', 'Laura', 'Cynthia', 'Kathleen', 'Amy', 'Shirley', 'Angela', 'Helen', 
		'Anna', 'Brenda', 'Pamela', 'Nicole', 'Emma', 'Samantha', 'Katherine', 'Christine', 'Debra', 
		'Rachel', 'Catherine', 'Carolyn', 'Janet', 'Ruth', 'Maria', 'Heather', 'Diane', 'Virginia', 
		'Julie', 'Joyce', 'Victoria', 'Olivia', 'Kelly', 'Christina', 'Lauren', 'Joan', 'Evelyn', 
		'Judith', 'Megan', 'Cheryl', 'Andrea', 'Hannah', 'Martha', 'Jacqueline', 'Frances', 'Gloria', 
		'Ann', 'Teresa', 'Kathryn', 'Sara', 'Janice', 'Jean', 'Alice', 'Madison', 'Doris', 'Abigail', 
		'Julia', 'Judy', 'Grace', 'Denise', 'Amber', 'Marilyn', 'Beverly', 'Danielle', 'Theresa', 
		'Sophia', 'Marie', 'Diana', 'Brittany', 'Natalie', 'Isabella', 'Charlotte', 'Rose', 'Alexis']

		for i in tqdm(range(200_000)):
			insert_users = (
				'''INSERT INTO airflow.users (ID_PRODUCTS, NAME, AGE, SEX, INCOME, DATE_ADDED ) 
				VALUES (%s, %s, %s, %s, %s, %s)'''
			)
			name = choice(names) + ' ' + choice(names)
			data = (randint(1, 8), 
					name, 
					randint(18, 80), 
					choice(['M','F']), 
					round(uniform(500.0, 50000.0), 2),
					datetime.datetime.fromtimestamp(randint(1234567891, 1634567891)))
			cursor.execute(insert_users, data)

		conn.commit()
