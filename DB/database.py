from sqlalchemy import create_engine, Column, Integer, Float, String, Date, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Ticker(Base):
    __tablename__ = 'tickers'
    
    ticker_id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    name = Column(String)
    sector = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(Date, default=datetime.now)
    
    # Relationships
    price_data = relationship("PriceData", back_populates="ticker")
    correlations_a = relationship("HighCorrelation", foreign_keys="HighCorrelation.ticker_a_id", back_populates="ticker_a")
    correlations_b = relationship("HighCorrelation", foreign_keys="HighCorrelation.ticker_b_id", back_populates="ticker_b")

class PriceData(Base):
    __tablename__ = 'price_data'
    
    price_id = Column(Integer, primary_key=True)
    ticker_id = Column(Integer, ForeignKey('tickers.ticker_id'))
    date = Column(Date)
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    volume = Column(Integer)
    created_at = Column(Date, default=datetime.now)
    
    # Relationships
    ticker = relationship("Ticker", back_populates="price_data")
    log_prices = relationship("LogPrice", back_populates="price_data")
    
    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uix_ticker_date'),
    )

class LogPrice(Base):
    __tablename__ = 'log_prices'
    
    log_id = Column(Integer, primary_key=True)
    price_id = Column(Integer, ForeignKey('price_data.price_id'))
    log_price = Column(Float)
    rolling_std = Column(Float)
    created_at = Column(Date, default=datetime.now)
    
    # Relationships
    price_data = relationship("PriceData", back_populates="log_prices")
    
    # Property to get date from price_data
    @property
    def date(self):
        return self.price_data.date if self.price_data else None

class HighCorrelation(Base):
    __tablename__ = 'high_correlations'
    
    pair_id = Column(Integer, primary_key=True)
    ticker_a_id = Column(Integer, ForeignKey('tickers.ticker_id'))
    ticker_b_id = Column(Integer, ForeignKey('tickers.ticker_id'))
    correlation = Column(Float)
    window_size = Column(Integer)
    created_at = Column(Date, default=datetime.now)
    
    # Relationships
    ticker_a = relationship("Ticker", foreign_keys=[ticker_a_id], back_populates="correlations_a")
    ticker_b = relationship("Ticker", foreign_keys=[ticker_b_id], back_populates="correlations_b")
    cointegration_tests = relationship("CointegrationTest", back_populates="pair")
    granger_tests = relationship("GrangerTest", back_populates="pair")
    trading_signals = relationship("TradingSignal", back_populates="pair")
    
    __table_args__ = (
        UniqueConstraint('ticker_a_id', 'ticker_b_id', 'window_size', name='uix_pair_window'),
    )

class CointegrationTest(Base):
    __tablename__ = 'cointegration_tests'
    
    test_id = Column(Integer, primary_key=True)
    pair_id = Column(Integer, ForeignKey('high_correlations.pair_id'))
    p_value = Column(Float)
    beta = Column(Float)
    test_date = Column(Date, default=datetime.now)
    created_at = Column(Date, default=datetime.now)
    
    # Relationships
    pair = relationship("HighCorrelation", back_populates="cointegration_tests")
    residuals = relationship("Residual", back_populates="test")

class GrangerTest(Base):
    __tablename__ = 'granger_tests'
    
    granger_id = Column(Integer, primary_key=True)
    pair_id = Column(Integer, ForeignKey('high_correlations.pair_id'))
    direction = Column(String)  # 'A->B' or 'B->A'
    p_value = Column(Float)
    lag_order = Column(Integer)
    test_date = Column(Date, default=datetime.now)
    created_at = Column(Date, default=datetime.now)
    
    # Relationships
    pair = relationship("HighCorrelation", back_populates="granger_tests")

class Residual(Base):
    __tablename__ = 'residuals'
    
    residual_id = Column(Integer, primary_key=True)
    test_id = Column(Integer, ForeignKey('cointegration_tests.test_id'))
    date = Column(Date)
    epsilon = Column(Float)
    z_score = Column(Float)
    created_at = Column(Date, default=datetime.now)
    
    # Relationships
    test = relationship("CointegrationTest", back_populates="residuals")
    
    __table_args__ = (
        UniqueConstraint('test_id', 'date', name='uix_test_date'),
    )

class TradingSignal(Base):
    __tablename__ = 'trading_signals'
    
    signal_id = Column(Integer, primary_key=True)
    pair_id = Column(Integer, ForeignKey('high_correlations.pair_id'))
    signal_type = Column(String)  # 'ENTRY' or 'EXIT'
    direction = Column(String)  # 'LONG_A_SHORT_B' or 'SHORT_A_LONG_B'
    z_score = Column(Float)
    created_at = Column(Date, default=datetime.now)
    status = Column(String)  # 'PENDING', 'EXECUTED', 'CANCELLED'
    
    # Relationships
    pair = relationship("HighCorrelation", back_populates="trading_signals")
    trades = relationship("Trade", back_populates="signal")

class Trade(Base):
    __tablename__ = 'trades'
    
    trade_id = Column(Integer, primary_key=True)
    signal_id = Column(Integer, ForeignKey('trading_signals.signal_id'))
    entry_date = Column(Date)
    exit_date = Column(Date)
    entry_price_a = Column(Float)
    entry_price_b = Column(Float)
    exit_price_a = Column(Float)
    exit_price_b = Column(Float)
    position_size = Column(Float)
    pnl = Column(Float)
    status = Column(String)  # 'OPEN', 'CLOSED', 'STOPPED'
    created_at = Column(Date, default=datetime.now)
    
    # Relationships
    signal = relationship("TradingSignal", back_populates="trades")

class PortfolioMetrics(Base):
    __tablename__ = 'portfolio_metrics'
    
    metric_id = Column(Integer, primary_key=True)
    date = Column(Date)
    total_value = Column(Float)
    cash_balance = Column(Float)
    open_pnl = Column(Float)
    closed_pnl = Column(Float)
    sharpe_ratio = Column(Float)
    max_drawdown = Column(Float)
    created_at = Column(Date, default=datetime.now)
    
    __table_args__ = (
        UniqueConstraint('date', name='uix_date'),
    )

def create_database(db_url='sqlite:///quant_trading.db'):
    """Create the database and all tables"""
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine

if __name__ == "__main__":
    # Create the database
    engine = create_database()
    print("Database and tables created successfully!") 