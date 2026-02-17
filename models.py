from sqlalchemy import Column, String, Integer, BigInteger, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True)
    partner_id = Column(String)
    name = Column(String)
    type = Column(String)
    path = Column(String)
    file_count = Column(Integer)
    total_size_bytes = Column(BigInteger)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
