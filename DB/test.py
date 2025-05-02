from database import Database

# Initialize the Database object (optionally pass a custom path)
db = Database()  # or Database('/path/to/your/QUANT.db')

# Connect to the database
db.connect()

# Create all tables (including trade_window)
db.create_tables()

db.close()