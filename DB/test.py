from database import Database

# Initialize the Database object (optionally pass a custom path)
db = Database()  # or Database('/path/to/your/QUANT.db')

# Connect to the database
db.connect()


db.create_tables()

db.close()