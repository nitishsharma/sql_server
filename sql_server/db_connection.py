from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Replace with your actual database URL
DATABASE_URL = "postgresql://username:password@localhost/dbname"

# Create an SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a new session instance when needed
db_session = SessionLocal()
