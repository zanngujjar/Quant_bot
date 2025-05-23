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

    def drop_15min_table(self) -> None:
        """Drop only the ticker_prices_15min table"""
        try:
            self.cursor.execute("DROP TABLE IF EXISTS ticker_prices_15min")
            self.conn.commit()
            print("ticker_prices_15min table dropped successfully!")
        except sqlite3.Error as e:
            print(f"Error dropping ticker_prices_15min table: {e}")
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
                ticker_1_logprice_id     INTEGER   NOT NULL,
                ticker_2_logprice_id     INTEGER   NOT NULL,
                ticker_id_1              INTEGER   NOT NULL,
                ticker_id_2              INTEGER   NOT NULL,
                epsilon                  REAL      NOT NULL,
                rolling_mean             REAL      NOT NULL,
                rolling_std              REAL      NOT NULL,
                z_score                  REAL      NOT NULL,
                date                     DATE      NOT NULL,
                
                CONSTRAINT uix_epsilon_price
                    UNIQUE (ticker_1_logprice_id, ticker_2_logprice_id),
                FOREIGN KEY (ticker_1_logprice_id) REFERENCES log_prices(id),
                FOREIGN KEY (ticker_2_logprice_id) REFERENCES log_prices(id),
                FOREIGN KEY (ticker_id_1)          REFERENCES tickers(id),
                FOREIGN KEY (ticker_id_2)          REFERENCES tickers(id)
            )
            """)
            
            # Create trade_window table
            #TRADE_TYPE: short A / long B true //// short B / long A false
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_window (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker_id1 INTEGER NOT NULL,
                ticker_id2 INTEGER NOT NULL,
                optimal_zscore REAL,
                reversion_success BOOL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                trade_type BOOL, 
                UNIQUE (ticker_id1, ticker_id2, start_date, end_date),
                FOREIGN KEY (ticker_id1) REFERENCES tickers(id),
                FOREIGN KEY (ticker_id2) REFERENCES tickers(id)
            )
            """)
            
            # Create option_snapshots table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS option_snapshots (
                    option_symbol      TEXT,
                    ticker_id         INTEGER NOT NULL,
                    underlying         TEXT,
                    quote_date         DATE,
                    expiration_date    DATE,
                    strike_price       NUMERIC(10,2),
                    contract_type      CHAR(1),        -- 'C' or 'P'
                    bid                NUMERIC(10,4),
                    ask                NUMERIC(10,4),
                    last               NUMERIC(10,4),
                    volume             INT,
                    open_interest      INT,
                    implied_volatility NUMERIC(9,6),
                    delta              NUMERIC(8,5),
                    gamma              NUMERIC(8,5),
                    theta              NUMERIC(8,5),
                    vega               NUMERIC(8,5),
                    underlying_price   NUMERIC(10,4),
                    PRIMARY KEY (option_symbol, ticker_id),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id),
                    UNIQUE (option_symbol, ticker_id)
                )
            """)
            
            # Create risk_free_rates table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS risk_free_rates (
                    observation_date DATE PRIMARY KEY,
                    rate            NUMERIC(10,4) NOT NULL
                )
            """)
            
            # Create dividends table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS dividends (
                    ticker          TEXT        NOT NULL,     
                    ex_div_date     DATE        NOT NULL,      
                    cash_amount     NUMERIC(10,4)  NOT NULL,   
                    dividend_type   CHAR(2)     NOT NULL,      
                    frequency       SMALLINT    NOT NULL,   
                    PRIMARY KEY (ticker, ex_div_date)
                )
            """)
            
            # Create ticker_prices_15min table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS ticker_prices_15min (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker_id      INTEGER NOT NULL,
                    ticker_symbol  TEXT NOT NULL,
                    bar_date       DATE NOT NULL,
                    bar_time       TEXT NOT NULL,
                    open_price     REAL NOT NULL,
                    high_price     REAL NOT NULL,
                    low_price      REAL NOT NULL,
                    close_price    REAL NOT NULL,
                    volume         INTEGER NOT NULL,
                    nanosecond     BIGINT,
                    UNIQUE(ticker_id, bar_date, bar_time),
                    FOREIGN KEY (ticker_id) REFERENCES tickers(id)
                )
            """)
            
            # Create indexes for ticker_prices_15min
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bars_ticker_datetime 
                ON ticker_prices_15min(ticker_id, bar_date, bar_time)
            """)
            
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bars_date_time 
                ON ticker_prices_15min(bar_date, bar_time)
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

    def add_epsilon_price(self, ticker_1_logprice_id: int, ticker_2_logprice_id: int,
                         ticker_id_1: int, ticker_id_2: int, epsilon: float,
                         rolling_mean: float, rolling_std: float, z_score: float,
                         date: str) -> None:
        """
        Add epsilon price data for a ticker pair with rolling statistics
        
        Args:
            ticker_1_logprice_id: ID of the first ticker's log price
            ticker_2_logprice_id: ID of the second ticker's log price
            ticker_id_1: ID of the first ticker
            ticker_id_2: ID of the second ticker
            epsilon: The epsilon value
            rolling_mean: Rolling mean of the spread
            rolling_std: Rolling standard deviation of the spread
            z_score: Z-score of the spread
            date: Date of the observation
        """
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO epsilon_prices 
                (ticker_1_logprice_id, ticker_2_logprice_id, ticker_id_1, ticker_id_2,
                 epsilon, rolling_mean, rolling_std, z_score, date) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ticker_1_logprice_id, ticker_2_logprice_id, ticker_id_1, ticker_id_2,
                  epsilon, rolling_mean, rolling_std, z_score, date))
            
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

    def get_ticker_symbol(self, ticker_id: int) -> Optional[str]:
        """Get the symbol of a ticker by its ID
        
        Args:
            ticker_id: The ID from the tickers table
            
        Returns:
            The ticker symbol if found, None otherwise
        """
        try:
            self.cursor.execute("SELECT symbol FROM tickers WHERE id = ?", (ticker_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
            
        except sqlite3.Error as e:
            print(f"Error getting ticker symbol: {e}")
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
        """Get epsilon price data for a pair of tickers within a date range
        
        Args:
            ticker_id_1: ID of the first ticker
            ticker_id_2: ID of the second ticker
            start_date: Optional start date filter (inclusive)
            end_date: Optional end date filter (inclusive)
            
        Returns:
            List of tuples containing (date, epsilon, rolling_mean, rolling_std, z_score)
        """
        try:
            query = """
                SELECT ep.date, ep.epsilon, ep.rolling_mean, ep.rolling_std, ep.z_score
                FROM epsilon_prices ep
                WHERE ep.ticker_id_1 = ? AND ep.ticker_id_2 = ?
            """
            params = [ticker_id_1, ticker_id_2]
            
            if start_date:
                query += " AND ep.date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND ep.date <= ?"
                params.append(end_date)
                
            query += " ORDER BY ep.date"
            
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

    def get_latest_cointegrated_pairs(self, max_p_value: float = 0.05) -> List[Tuple[str, str, float]]:
        """Get all ticker pairs that are cointegrated below the p-value threshold from the most recent test date
        
        Returns:
            List of tuples containing (ticker1_symbol, ticker2_symbol, beta)
        """
        try:
            self.cursor.execute("""
                WITH latest_test_date AS (
                    SELECT MAX(test_date) as max_date
                    FROM cointegration_tests
                )
                SELECT DISTINCT t1.symbol, t2.symbol, ct.beta
                FROM cointegration_tests ct
                JOIN tickers t1 ON ct.ticker_id_1 = t1.id
                JOIN tickers t2 ON ct.ticker_id_2 = t2.id
                JOIN latest_test_date ltd ON ct.test_date = ltd.max_date
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

    def add_epsilon_prices_batch(self, epsilon_data: List[Tuple]) -> None:
        """
        Add multiple epsilon price records in a single transaction for better performance.
        
        Args:
            epsilon_data: List of tuples, each containing:
                (ticker_1_logprice_id, ticker_2_logprice_id, ticker_id_1, ticker_id_2,
                 epsilon, rolling_mean, rolling_std, z_score, date)
        """
        try:
            self.cursor.executemany("""
                INSERT OR REPLACE INTO epsilon_prices 
                (ticker_1_logprice_id, ticker_2_logprice_id, ticker_id_1, ticker_id_2,
                 epsilon, rolling_mean, rolling_std, z_score, date) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, epsilon_data)
            
            self.conn.commit()
            
        except sqlite3.Error as e:
            print(f"Error adding epsilon prices in batch: {e}")
            self.conn.rollback()
            raise

    def get_log_price_ids_batch(self, ticker_price_ids: List[int]) -> Dict[int, int]:
        """
        Get log price IDs for a batch of ticker price IDs
        
        Args:
            ticker_price_ids: List of ticker_price_id values
            
        Returns:
            Dictionary mapping ticker_price_id to log_price.id
        """
        try:
            if not ticker_price_ids:
                return {}
                
            placeholders = ','.join(['?' for _ in ticker_price_ids])
            query = f"""
                SELECT ticker_price_id, id
                FROM log_prices
                WHERE ticker_price_id IN ({placeholders})
            """
            
            self.cursor.execute(query, ticker_price_ids)
            return dict(self.cursor.fetchall())
            
        except sqlite3.Error as e:
            print(f"Error getting log price IDs in batch: {e}")
            raise

    def get_log_price_id_from_ticker_price(self, ticker_price_id: int) -> Optional[int]:
        """
        Get log price ID for a given ticker_price_id
        
        Args:
            ticker_price_id: The ID from the ticker_prices table
            
        Returns:
            The log price ID if found, None otherwise
        """
        try:
            query = """
                SELECT id
                FROM log_prices
                WHERE ticker_price_id = ?
            """
            
            self.cursor.execute(query, (ticker_price_id,))
            result = self.cursor.fetchone()
            return result[0] if result else None
            
        except sqlite3.Error as e:
            print(f"Error getting log price ID: {e}")
            raise

    def get_ticker_price_id(self, ticker_id: int, date: str) -> Optional[int]:
        """
        Get ticker price ID for a given ticker_id and date
        
        Args:
            ticker_id: The ID from the tickers table
            date: The date string in YYYY-MM-DD format
            
        Returns:
            The ticker price ID if found, None otherwise
        """
        try:
            query = """
                SELECT id
                FROM ticker_prices
                WHERE ticker_id = ? AND date = ?
            """
            
            self.cursor.execute(query, (ticker_id, date))
            result = self.cursor.fetchone()
            return result[0] if result else None
            
        except sqlite3.Error as e:
            print(f"Error getting ticker price ID: {e}")
            raise

    def get_epsilon_ticker_pairs(self) -> List[List[int]]:
        """
        Get all unique ticker pairs from epsilon_prices table
        
        Returns:
            List of [ticker_id_1, ticker_id_2] pairs
        """
        try:
            query = """
                SELECT DISTINCT ticker_id_1, ticker_id_2
                FROM epsilon_prices
                ORDER BY ticker_id_1, ticker_id_2
            """
            
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return [[pair[0], pair[1]] for pair in results]
            
        except sqlite3.Error as e:
            print(f"Error getting epsilon ticker pairs: {e}")
            raise

    def add_trade_window(self, ticker_id1: int, ticker_id2: int, optimal_zscore: float,
                         reversion_success: bool, start_date: str, end_date: str, trade_type: bool) -> int:
        """Insert a single trade window record and return its id."""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO trade_window
                (ticker_id1, ticker_id2, optimal_zscore, reversion_success, start_date, end_date, trade_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ticker_id1, ticker_id2, optimal_zscore, reversion_success, start_date, end_date, trade_type))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Error adding trade window: {e}")
            self.conn.rollback()
            raise

    def add_trade_windows_batch(self, trade_windows: List[tuple]) -> None:
        """
        Bulk insert trade window records.
        Each tuple should be:
        (ticker_id1, ticker_id2, optimal_zscore, reversion_success, start_date, end_date, trade_type)
        """
        try:
            self.cursor.executemany("""
                INSERT OR REPLACE INTO trade_window
                (ticker_id1, ticker_id2, optimal_zscore, reversion_success, start_date, end_date, trade_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, trade_windows)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding trade windows in batch: {e}")
            self.conn.rollback()
            raise

    def get_trade_windows(self, ticker_id1: int = None, ticker_id2: int = None) -> List[Dict[str, any]]:
        """
        Retrieve trade window records, optionally filtered by ticker IDs.
        Returns a list of dicts.
        """
        try:
            query = "SELECT * FROM trade_window"
            params = []
            if ticker_id1 is not None and ticker_id2 is not None:
                query += " WHERE ticker_id1 = ? AND ticker_id2 = ?"
                params = [ticker_id1, ticker_id2]
            elif ticker_id1 is not None:
                query += " WHERE ticker_id1 = ?"
                params = [ticker_id1]
            elif ticker_id2 is not None:
                query += " WHERE ticker_id2 = ?"
                params = [ticker_id2]
            self.cursor.execute(query, params)
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error getting trade windows: {e}")
            raise

    def get_latest_risk_free_rate(self) -> Optional[Tuple[str, float]]:
        """
        Get the most recent risk-free rate record.
        
        Returns:
            Tuple of (date, rate) for the most recent record, or None if no records exist
        """
        try:
            self.cursor.execute("""
                SELECT observation_date, rate 
                FROM risk_free_rates 
                ORDER BY observation_date DESC 
                LIMIT 1
            """)
            
            return self.cursor.fetchone()
            
        except sqlite3.Error as e:
            print(f"Error getting latest risk-free rate: {e}")
            raise

    def set_risk_free_rate(self, date: str, rate: float) -> None:
        """
        Set or update the risk-free rate for a specific date.
        
        Args:
            date: The date in YYYY-MM-DD format
            rate: The risk-free rate value
        """
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO risk_free_rates (observation_date, rate)
                VALUES (?, ?)
            """, (date, rate))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error setting risk-free rate: {e}")
            self.conn.rollback()
            raise

    def get_risk_free_rate(self, date: str) -> Optional[float]:
        """
        Get the risk-free rate for a specific date.
        
        Args:
            date: The date in YYYY-MM-DD format
            
        Returns:
            The risk-free rate value if found, None otherwise
        """
        try:
            self.cursor.execute("""
                SELECT rate FROM risk_free_rates 
                WHERE observation_date = ?
            """, (date,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            print(f"Error getting risk-free rate: {e}")
            raise

    def get_all_risk_free_rates(self) -> List[Tuple[str, float]]:
        """
        Get all risk-free rates in the database.
        
        Returns:
            List of tuples containing (date, rate) ordered by date
        """
        try:
            self.cursor.execute("""
                SELECT observation_date, rate 
                FROM risk_free_rates 
                ORDER BY observation_date
            """)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error getting all risk-free rates: {e}")
            raise

    def get_all_tickers(self) -> List[Tuple[int, str]]:
        """
        Get all ticker IDs and symbols from the database.
        
        Returns:
            List of tuples containing (ticker_id, symbol)
        """
        try:
            self.cursor.execute("""
                SELECT id, symbol
                FROM tickers
                ORDER BY symbol
            """)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error getting all tickers: {e}")
            raise

    def add_dividend(self, ticker: str, ex_div_date: str, cash_amount: float, 
                    dividend_type: str, frequency: int) -> None:
        """
        Add a single dividend record to the database.
        
        Args:
            ticker: Stock symbol
            ex_div_date: Ex-dividend date in YYYY-MM-DD format
            cash_amount: Per-share cash amount
            dividend_type: Type of dividend ('CD', 'SC', 'LT', 'ST')
            frequency: Dividend frequency (1, 4, 12, etc.)
        """
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO dividends 
                (ticker, ex_div_date, cash_amount, dividend_type, frequency)
                VALUES (?, ?, ?, ?, ?)
            """, (ticker, ex_div_date, cash_amount, dividend_type, frequency))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding dividend: {e}")
            self.conn.rollback()
            raise

    def add_dividends_batch(self, dividends: List[Tuple[str, str, float, str, int]]) -> None:
        """
        Add multiple dividend records in a single transaction.
        
        Args:
            dividends: List of tuples, each containing:
                (ticker, ex_div_date, cash_amount, dividend_type, frequency)
        """
        try:
            self.cursor.executemany("""
                INSERT OR REPLACE INTO dividends 
                (ticker, ex_div_date, cash_amount, dividend_type, frequency)
                VALUES (?, ?, ?, ?, ?)
            """, dividends)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding dividends in batch: {e}")
            self.conn.rollback()
            raise

    def get_dividends(self, ticker: Optional[str] = None, 
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> List[Tuple]:
        """
        Get dividend records, optionally filtered by ticker and date range.
        
        Args:
            ticker: Optional ticker symbol to filter by
            start_date: Optional start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format
            
        Returns:
            List of tuples containing (ticker, ex_div_date, cash_amount, dividend_type, frequency)
        """
        try:
            query = "SELECT * FROM dividends"
            params = []
            
            conditions = []
            if ticker:
                conditions.append("ticker = ?")
                params.append(ticker)
            if start_date:
                conditions.append("ex_div_date >= ?")
                params.append(start_date)
            if end_date:
                conditions.append("ex_div_date <= ?")
                params.append(end_date)
                
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
                
            query += " ORDER BY ticker, ex_div_date DESC"
            
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"Error getting dividends: {e}")
            raise

    def get_latest_dividend(self, ticker: str) -> Optional[Tuple]:
        """
        Get the most recent dividend record for a ticker.
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Tuple containing (ticker, ex_div_date, cash_amount, dividend_type, frequency)
            or None if no dividend found
        """
        try:
            self.cursor.execute("""
                SELECT * FROM dividends 
                WHERE ticker = ? 
                ORDER BY ex_div_date DESC 
                LIMIT 1
            """, (ticker,))
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Error getting latest dividend: {e}")
            raise

    def get_dividend_frequency(self, ticker: str) -> Optional[int]:
        """
        Get the most common dividend frequency for a ticker.
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Most common frequency value or None if no dividends found
        """
        try:
            self.cursor.execute("""
                SELECT frequency, COUNT(*) as count
                FROM dividends
                WHERE ticker = ?
                GROUP BY frequency
                ORDER BY count DESC
                LIMIT 1
            """, (ticker,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            print(f"Error getting dividend frequency: {e}")
            raise

    def add_15min_price(self, ticker_id: int, ticker_symbol: str, bar_date: str, bar_time: str,
                       open_price: float, high_price: float, low_price: float, 
                       close_price: float, volume: int, nanosecond: int) -> int:
        """
        Add a single 15-minute price record to the database.
        
        Args:
            ticker_id: ID of the ticker
            ticker_symbol: Symbol of the ticker
            bar_date: Date of the bar in YYYY-MM-DD format
            bar_time: Time of the bar in HH:MM:SS format
            open_price: Opening price
            high_price: Highest price
            low_price: Lowest price
            close_price: Closing price
            volume: Trading volume
            nanosecond: Nanosecond timestamp
            
        Returns:
            The ID of the inserted record
        """
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO ticker_prices_15min 
                (ticker_id, ticker_symbol, bar_date, bar_time, 
                 open_price, high_price, low_price, close_price, volume, nanosecond)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ticker_id, ticker_symbol, bar_date, bar_time,
                  open_price, high_price, low_price, close_price, volume, nanosecond))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Error adding 15min price: {e}")
            self.conn.rollback()
            raise

    def add_15min_prices_batch(self, prices: List[Tuple]) -> None:
        """
        Add multiple 15-minute price records in a single transaction.
        
        Args:
            prices: List of tuples, each containing:
                (ticker_id, ticker_symbol, bar_date, bar_time, 
                 open_price, high_price, low_price, close_price, volume, nanosecond)
        """
        try:
            self.cursor.executemany("""
                INSERT OR REPLACE INTO ticker_prices_15min 
                (ticker_id, ticker_symbol, bar_date, bar_time, 
                 open_price, high_price, low_price, close_price, volume, nanosecond)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, prices)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding 15min prices in batch: {e}")
            self.conn.rollback()
            raise

    def get_15min_prices_by_ticker(self, ticker_id: int, 
                                 start_date: Optional[str] = None,
                                 end_date: Optional[str] = None,
                                 start_nanosecond: Optional[int] = None,
                                 end_nanosecond: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get 15-minute price data for a specific ticker within a date range.
        
        Args:
            ticker_id: ID of the ticker
            start_date: Optional start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format
            start_nanosecond: Optional start nanosecond timestamp
            end_nanosecond: Optional end nanosecond timestamp
            
        Returns:
            List of dictionaries containing price data
        """
        try:
            query = """
                SELECT * FROM ticker_prices_15min
                WHERE ticker_id = ?
            """
            params = [ticker_id]
            
            if start_date:
                query += " AND (bar_date > ? OR (bar_date = ? AND (bar_time > ? OR (bar_time = ? AND nanosecond >= ?))))"
                params.extend([start_date, start_date, start_time, start_time, start_nanosecond])
            if end_date:
                query += " AND (bar_date < ? OR (bar_date = ? AND (bar_time < ? OR (bar_time = ? AND nanosecond <= ?))))"
                params.extend([end_date, end_date, end_time, end_time, end_nanosecond])
                
            query += " ORDER BY bar_date, bar_time, nanosecond"
            
            self.cursor.execute(query, params)
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            
        except sqlite3.Error as e:
            print(f"Error getting 15min prices by ticker: {e}")
            raise

    def get_15min_prices_by_date_range(self, start_date: str, end_date: str,
                                     start_time: Optional[str] = None,
                                     end_time: Optional[str] = None,
                                     start_nanosecond: Optional[int] = None,
                                     end_nanosecond: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get 15-minute price data within a date and time range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            start_time: Optional start time in HH:MM:SS format
            end_time: Optional end time in HH:MM:SS format
            start_nanosecond: Optional start nanosecond timestamp
            end_nanosecond: Optional end nanosecond timestamp
            
        Returns:
            List of dictionaries containing price data
        """
        try:
            query = """
                SELECT * FROM ticker_prices_15min
                WHERE bar_date >= ? AND bar_date <= ?
            """
            params = [start_date, end_date]
            
            if start_time:
                query += " AND (bar_date > ? OR (bar_date = ? AND (bar_time > ? OR (bar_time = ? AND nanosecond >= ?))))"
                params.extend([start_date, start_date, start_time, start_time, start_nanosecond])
            if end_time:
                query += " AND (bar_date < ? OR (bar_date = ? AND (bar_time < ? OR (bar_time = ? AND nanosecond <= ?))))"
                params.extend([end_date, end_date, end_time, end_time, end_nanosecond])
                
            query += " ORDER BY bar_date, bar_time, nanosecond"
            
            self.cursor.execute(query, params)
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            
        except sqlite3.Error as e:
            print(f"Error getting 15min prices by date range: {e}")
            raise

    def get_latest_15min_price(self, ticker_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the most recent 15-minute price record for a ticker.
        
        Args:
            ticker_id: ID of the ticker
            
        Returns:
            Dictionary containing the latest price data or None if no data exists
        """
        try:
            self.cursor.execute("""
                SELECT * FROM ticker_prices_15min
                WHERE ticker_id = ?
                ORDER BY bar_date DESC, bar_time DESC, nanosecond DESC
                LIMIT 1
            """, (ticker_id,))
            
            result = self.cursor.fetchone()
            if result:
                columns = [desc[0] for desc in self.cursor.description]
                return dict(zip(columns, result))
            return None
            
        except sqlite3.Error as e:
            print(f"Error getting latest 15min price: {e}")
            raise

