from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, create_engine, MetaData
from sqlalchemy.orm import relationship, sessionmaker, declarative_base, mapped_column, Mapped, DeclarativeBase
from typing import List, Optional, Dict, Any
from datetime import datetime

# Create base class for SQLAlchemy 2.0
class Base(DeclarativeBase):
    pass

# Source database model
class SourceModel(Base):
    __tablename__ = 'source_table'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    column1: Mapped[str] = mapped_column(String(100), nullable=False)
    column2: Mapped[Optional[str]] = mapped_column(String(200))
    column3: Mapped[Optional[float]] = mapped_column(Float)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # Relationship with details
    details: Mapped[List["SourceDetailModel"]] = relationship(back_populates="source")
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'column1': self.column1,
            'column2': self.column2,
            'column3': self.column3,
            'created_at': self.created_at
        }

# Target database model
class TargetModel(Base):
    __tablename__ = 'target_table'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    column1: Mapped[str] = mapped_column(String(100), nullable=False)
    column2: Mapped[Optional[str]] = mapped_column(String(200))
    column3_transformed: Mapped[Optional[float]] = mapped_column(Float)
    migrated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # Relationship with details
    details: Mapped[List["TargetDetailModel"]] = relationship(back_populates="target")
    
    @classmethod
    def from_source(cls, source_dict: Dict[str, Any], migrated_at: datetime) -> "TargetModel":
        return cls(
            id=source_dict['id'],
            column1=source_dict['column1'],
            column2=source_dict['column2'],
            column3_transformed=source_dict['column3_transformed'],
            migrated_at=migrated_at
        )

# Additional related model example for source
class SourceDetailModel(Base):
    __tablename__ = 'source_detail'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey('source_table.id'))
    detail_data: Mapped[Optional[str]] = mapped_column(String(500))
    
    # Bidirectional relationship
    source: Mapped["SourceModel"] = relationship(back_populates="details")

# Additional related model example for target
class TargetDetailModel(Base):
    __tablename__ = 'target_detail'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    target_id: Mapped[int] = mapped_column(ForeignKey('target_table.id'))
    detail_data: Mapped[Optional[str]] = mapped_column(String(500))
    
    # Bidirectional relationship
    target: Mapped["TargetModel"] = relationship(back_populates="details")

def init_db(engine) -> None:
    """Initialize database tables"""
    Base.metadata.create_all(engine)

def get_session(engine):
    """Create a session factory"""
    Session = sessionmaker(bind=engine)
    return Session() 