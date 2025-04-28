import sqlite3
import os
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime

class Database:
    def __init__(self, db_path: Optional[str] = None):
        """Initialize the database connection"""
        if db_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(script_dir, 'QUANT.db')
        
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self) -> None:
        """Establish connection to the database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            self.cursor.execute("PRAGMA foreign_keys = ON")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def close(self) -> None:
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def drop_tables(self) -> None:
        """Drop all tables in the database"""
        try:
            # Drop tables in reverse order of dependencies
            self.cursor.execute("DROP TABLE IF EXISTS epsilon_prices")
            self.cursor.execute("DROP TABLE IF EXISTS cointegration_tests")
            self.cursor.execute("DROP TABLE IF EXISTS high_correlations")
            self.cursor.execute("DROP TABLE IF EXISTS log_prices")
            self.cursor.execute("DROP TABLE IF EXISTS ticker_prices")
            self.cursor.execute("DROP TABLE IF EXISTS tickers")
            
            self.conn.commit()
            print("All tables dropped successfully!")
            
        except sqlite3.Error as e:
            print(f"Error dropping tables: {e}")
            self.conn.rollback()
            raise

    def create_tables(self) -> None:
        """Create all required tables if they don't exist"""
        try:
            # Create tickers table
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol   TEXT    NOT NULL UNIQUE
            )
            """)
            
            # Create ticker_prices table
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ticker_prices (
                id          INTEGER   PRIMARY KEY AUTOINCREMENT,
                ticker_id   INTEGER   NOT NULL,
                date        DATE      NOT NULL,
                close_price REAL      NOT NULL,
                created_at  DATETIME  NOT NULL DEFAULT (datetime('now')),
                CONSTRAINT  uix_price_ticker_date UNIQUE (ticker_id, date),
                FOREIGN KEY (ticker_id) REFERENCES tickers(id)
            )
            """)
            
            # Create log_prices table
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS log_prices (
                id                  INTEGER   PRIMARY KEY,
                ticker_price_id     INTEGER   NOT NULL,
                log_price           REAL      NOT NULL,
                mean_30d            REAL,
                std_30d             REAL,
                mean_90d            REAL,
                std_90d             REAL,
                CONSTRAINT uix_logprice_price
                    UNIQUE (ticker_price_id),
                FOREIGN KEY (ticker_price_id)
                    REFERENCES ticker_prices(id)
            )
            """)
            
            # Create high_correlations table
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS high_correlations (
                id             INTEGER   PRIMARY KEY AUTOINCREMENT,
                ticker_id_1    INTEGER   NOT NULL,
                ticker_id_2    INTEGER   NOT NULL,
                correlation    REAL      NOT NULL,
                date           DATE      NOT NULL,
                CONSTRAINT     uix_highcorr_pair_date UNIQUE (ticker_id_1, ticker_id_2, date),
                FOREIGN KEY (ticker_id_1) REFERENCES tickers(id),
                FOREIGN KEY (ticker_id_2) REFERENCES tickers(id)
            )
            """)
            
            # Create cointegration_tests table
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS cointegration_tests (
                id             INTEGER   PRIMARY KEY AUTOINCREMENT,
                ticker_id_1    INTEGER   NOT NULL,
                ticker_id_2    INTEGER   NOT NULL,
                p_value        REAL,
                beta           REAL,
                test_date      DATE      NOT NULL,
                CONSTRAINT     uix_coint_pair_date UNIQUE (ticker_id_1, ticker_id_2, test_date),
                FOREIGN KEY (ticker_id_1) REFERENCES tickers(id),
                FOREIGN KEY (ticker_id_2) REFERENCES tickers(id)
            )
            """)
            
            # Create epsilon_prices table
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS epsilon_prices (
                id                       INTEGER   PRIMARY KEY AUTOINCREMENT,
                ticker_price_id          INTEGER   NOT NULL,
                ticker_id_1              INTEGER   NOT NULL,
                ticker_id_2              INTEGER   NOT NULL,
                epsilon                  REAL      NOT NULL,
                entry_threshold_z        REAL      NOT NULL,
                exit_threshold_z         REAL      NOT NULL,
                reversion_success_rate   REAL      NOT NULL,
                CONSTRAINT uix_epsilon_price
                    UNIQUE (ticker_price_id, ticker_id_1, ticker_id_2),
                FOREIGN KEY (ticker_price_id)    REFERENCES ticker_prices(id),
                FOREIGN KEY (ticker_id_1)        REFERENCES tickers(id),
                FOREIGN KEY (ticker_id_2)        REFERENCES tickers(id)
            )
            """)
            
            self.conn.commit()
            print("Database tables created successfully!")
            
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
            self.conn.rollback()
            raise

    def add_ticker(self, symbol: str) -> int:
        """Add a new ticker to the database"""
        try:
            self.cursor.execute("INSERT OR IGNORE INTO tickers (symbol) VALUES (?)", (symbol,))
            self.conn.commit()
            
            # Get the ticker ID
            self.cursor.execute("SELECT id FROM tickers WHERE symbol = ?", (symbol,))
            result = self.cursor.fetchone()
            return result[0] if result else None
            
        except sqlite3.Error as e:
            print(f"Error adding ticker: {e}")
            self.conn.rollback()
            raise

    def add_ticker_price(self, ticker_id: int, date: str, close_price: float) -> int:
        """Add a new price record for a ticker"""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO ticker_prices 
                (ticker_id, date, close_price) 
                VALUES (?, ?, ?)
            """, (ticker_id, date, close_price))
            
            self.conn.commit()
            
            # Get the price record ID
            self.cursor.execute("""
                SELECT id FROM ticker_prices 
                WHERE ticker_id = ? AND date = ?
            """, (ticker_id, date))
            
            result = self.cursor.fetchone()
            return result[0] if result else None
            
        except sqlite3.Error as e:
            print(f"Error adding ticker price: {e}")
            self.conn.rollback()
            raise

    def add_log_price(self, ticker_price_id: int, log_price: float, 
                     mean_30d: Optional[float] = None, std_30d: Optional[float] = None,
                     mean_90d: Optional[float] = None, std_90d: Optional[float] = None) -> None:
        """Add log price data for a ticker price record"""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO log_prices 
                (id, ticker_price_id, log_price, mean_30d, std_30d, mean_90d, std_90d) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ticker_price_id, ticker_price_id, log_price, mean_30d, std_30d, mean_90d, std_90d))
            
            self.conn.commit()
            
        except sqlite3.Error as e:
            print(f"Error adding log price: {e}")
            self.conn.rollback()
            raise

    def add_high_correlation(self, ticker_id_1: int, ticker_id_2: int, 
                           correlation: float, date: str) -> None:
        """Add a high correlation record between two tickers"""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO high_correlations 
                (ticker_id_1, ticker_id_2, correlation, date) 
                VALUES (?, ?, ?, ?)
            """, (ticker_id_1, ticker_id_2, correlation, date))
            
            self.conn.commit()
            
        except sqlite3.Error as e:
            print(f"Error adding high correlation: {e}")
            self.conn.rollback()
            raise

    def add_cointegration_test(self, ticker_id_1: int, ticker_id_2: int, 
                             p_value: float, beta: float, test_date: str) -> None:
        """Add cointegration test results for a pair of tickers"""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO cointegration_tests 
                (ticker_id_1, ticker_id_2, p_value, beta, test_date) 
                VALUES (?, ?, ?, ?, ?)
            """, (ticker_id_1, ticker_id_2, p_value, beta, test_date))
            
            self.conn.commit()
            
        except sqlite3.Error as e:
            print(f"Error adding cointegration test: {e}")
            self.conn.rollback()
            raise

    def add_epsilon_price(self, ticker_price_id: int, ticker_id_1: int, ticker_id_2: int,
                         epsilon: float, entry_threshold_z: float, 
                         exit_threshold_z: float, reversion_success_rate: float) -> None:
        """Add epsilon price data for a ticker pair"""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO epsilon_prices 
                (ticker_price_id, ticker_id_1, ticker_id_2, epsilon, 
                 entry_threshold_z, exit_threshold_z, reversion_success_rate) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ticker_price_id, ticker_id_1, ticker_id_2, epsilon,
                  entry_threshold_z, exit_threshold_z, reversion_success_rate))
            
            self.conn.commit()
            
        except sqlite3.Error as e:
            print(f"Error adding epsilon price: {e}")
            self.conn.rollback()
            raise

    def get_ticker_id(self, symbol: str) -> Optional[int]:
        """Get the ID of a ticker by its symbol"""
        try:
            self.cursor.execute("SELECT id FROM tickers WHERE symbol = ?", (symbol,))
            result = self.cursor.fetchone()
            return result[0] if result else None
            
        except sqlite3.Error as e:
            print(f"Error getting ticker ID: {e}")
            raise

    def get_ticker_prices(self, ticker_id: int, start_date: Optional[str] = None, 
                         end_date: Optional[str] = None) -> List[Tuple]:
        """Get price data for a ticker within a date range"""
        try:
            query = """
                SELECT date, close_price 
                FROM ticker_prices 
                WHERE ticker_id = ?
            """
            params = [ticker_id]
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
                
            query += " ORDER BY date"
            
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting ticker prices: {e}")
            raise

    def get_high_correlations(self, min_correlation: float = 0.7) -> List[Tuple]:
        """Get all high correlation pairs above a minimum threshold"""
        try:
            self.cursor.execute("""
                SELECT t1.symbol, t2.symbol, hc.correlation, hc.date
                FROM high_correlations hc
                JOIN tickers t1 ON hc.ticker_id_1 = t1.id
                JOIN tickers t2 ON hc.ticker_id_2 = t2.id
                WHERE hc.correlation >= ?
                ORDER BY hc.correlation DESC
            """, (min_correlation,))
            
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting high correlations: {e}")
            raise

    def get_cointegrated_pairs(self, max_p_value: float = 0.05) -> List[Tuple]:
        """Get all cointegrated pairs below a maximum p-value threshold"""
        try:
            self.cursor.execute("""
                SELECT t1.symbol, t2.symbol, ct.p_value, ct.beta, ct.test_date
                FROM cointegration_tests ct
                JOIN tickers t1 ON ct.ticker_id_1 = t1.id
                JOIN tickers t2 ON ct.ticker_id_2 = t2.id
                WHERE ct.p_value <= ?
                ORDER BY ct.p_value
            """, (max_p_value,))
            
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting cointegrated pairs: {e}")
            raise

    def get_epsilon_prices(self, ticker_id_1: int, ticker_id_2: int, 
                          start_date: Optional[str] = None, 
                          end_date: Optional[str] = None) -> List[Tuple]:
        """Get epsilon price data for a pair of tickers within a date range"""
        try:
            query = """
                SELECT tp.date, ep.epsilon, ep.entry_threshold_z, 
                       ep.exit_threshold_z, ep.reversion_success_rate
                FROM epsilon_prices ep
                JOIN ticker_prices tp ON ep.ticker_price_id = tp.id
                WHERE ep.ticker_id_1 = ? AND ep.ticker_id_2 = ?
            """
            params = [ticker_id_1, ticker_id_2]
            
            if start_date:
                query += " AND tp.date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND tp.date <= ?"
                params.append(end_date)
                
            query += " ORDER BY tp.date"
            
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting epsilon prices: {e}")
            raise

    def get_high_correlation_pairs(self, min_correlation: float = 0.8) -> List[Tuple[str, str]]:
        """Get all ticker pairs that have high correlation above the threshold
        
        Returns:
            List of tuples containing (ticker1_symbol, ticker2_symbol)
        """
        try:
            self.cursor.execute("""
                SELECT DISTINCT t1.symbol, t2.symbol
                FROM high_correlations hc
                JOIN tickers t1 ON hc.ticker_id_1 = t1.id
                JOIN tickers t2 ON hc.ticker_id_2 = t2.id
                WHERE hc.correlation >= ?
                ORDER BY t1.symbol, t2.symbol
            """, (min_correlation,))
            
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting high correlation pairs: {e}")
            raise

    def get_cointegrated_pairs(self, max_p_value: float = 0.05) -> List[Tuple[str, str]]:
        """Get all ticker pairs that are cointegrated below the p-value threshold
        
        Returns:
            List of tuples containing (ticker1_symbol, ticker2_symbol)
        """
        try:
            self.cursor.execute("""
                SELECT DISTINCT t1.symbol, t2.symbol
                FROM cointegration_tests ct
                JOIN tickers t1 ON ct.ticker_id_1 = t1.id
                JOIN tickers t2 ON ct.ticker_id_2 = t2.id
                WHERE ct.p_value <= ?
                ORDER BY t1.symbol, t2.symbol
            """, (max_p_value,))
            
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting cointegrated pairs: {e}")
            raise

    def get_latest_log_prices_for_pair(self, ticker1: str, ticker2: str, 
                                     days: int = 30) -> List[Tuple[str, float, float]]:
        """Get the most recent log prices for a pair of tickers
        
        Args:
            ticker1: First ticker symbol
            ticker2: Second ticker symbol
            days: Number of days of data to retrieve (default: 30)
            
        Returns:
            List of tuples containing (date, ticker1_log_price, ticker2_log_price)
        """
        try:
            query = """
                WITH latest_dates AS (
                    SELECT DISTINCT date
                    FROM ticker_prices tp
                    JOIN tickers t ON tp.ticker_id = t.id
                    WHERE t.symbol IN (?, ?)
                    ORDER BY date DESC
                    LIMIT ?
                )
                SELECT 
                    tp1.date,
                    lp1.log_price as log_price1,
                    lp2.log_price as log_price2
                FROM latest_dates ld
                JOIN ticker_prices tp1 ON tp1.date = ld.date
                JOIN ticker_prices tp2 ON tp2.date = ld.date
                JOIN log_prices lp1 ON tp1.id = lp1.ticker_price_id
                JOIN log_prices lp2 ON tp2.id = lp2.ticker_price_id
                JOIN tickers t1 ON tp1.ticker_id = t1.id
                JOIN tickers t2 ON tp2.ticker_id = t2.id
                WHERE t1.symbol = ? AND t2.symbol = ?
                ORDER BY tp1.date DESC
            """
            
            self.cursor.execute(query, (ticker1, ticker2, days, ticker1, ticker2))
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting latest log prices for pair: {e}")
            raise

    def get_latest_log_prices_for_pairs(self, pairs: List[Tuple[str, str]], 
                                      days: int = 30) -> Dict[Tuple[str, str], List[Tuple[str, float, float]]]:
        """Get the most recent log prices for multiple pairs of tickers
        
        Args:
            pairs: List of (ticker1, ticker2) tuples
            days: Number of days of data to retrieve (default: 30)
            
        Returns:
            Dictionary mapping (ticker1, ticker2) to list of (date, log_price1, log_price2) tuples
        """
        result = {}
        for ticker1, ticker2 in pairs:
            result[(ticker1, ticker2)] = self.get_latest_log_prices_for_pair(
                ticker1, ticker2, days
            )
        return result

    def get_missing_log_prices(self) -> List[Tuple[int, str, str, float]]:
        """Get all ticker price records that don't have corresponding log price entries
        
        Returns:
            List of tuples containing (price_id, ticker_symbol, date, close_price)
            for records that need log price calculation
        """
        try:
            query = """
                SELECT 
                    tp.id as price_id,
                    t.symbol,
                    tp.date,
                    tp.close_price
                FROM ticker_prices tp
                JOIN tickers t ON tp.ticker_id = t.id
                LEFT JOIN log_prices lp ON tp.id = lp.ticker_price_id
                WHERE lp.id IS NULL
                ORDER BY tp.date DESC, t.symbol
            """
            
            self.cursor.execute(query)
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting missing log prices: {e}")
            raise

    def get_latest_price_dates(self) -> List[Tuple[str, str]]:
        """Get the latest date for each unique ticker in the price data
        
        Returns:
            List of tuples containing (ticker_symbol, latest_date)
            sorted by ticker symbol
        """
        try:
            query = """
                SELECT 
                    t.symbol,
                    MAX(tp.date) as latest_date
                FROM ticker_prices tp
                JOIN tickers t ON tp.ticker_id = t.id
                GROUP BY t.symbol
                ORDER BY t.symbol
            """
            
            self.cursor.execute(query)
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting latest price dates: {e}")
            raise


