# Quant Trading Bot

A quantitative trading bot that implements pairs trading strategy using cointegration and Granger causality tests.

## Features

- Automated price data collection
- Correlation analysis
- Cointegration testing
- Granger causality testing
- Signal generation
- Risk management

## Setup

1. Clone the repository:
```bash
git clone https://github.com/zanngujjar/Quant_bot.git
cd Quant_bot
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file:
```bash
cp .env.template .env
```

4. Edit the `.env` file with your configuration:
- Add your Polygon.io API key
- Adjust trading parameters as needed

5. Initialize the database:
```bash
python database.py
```

## Usage

1. Populate tickers:
```bash
python populate_tickers.py
```

2. Run the main bot:
```bash
python main.py
```

## Project Structure

- `database.py`: Database schema and initialization
- `update_prices.py`: Price data collection
- `check_correlations.py`: Correlation analysis
- `granger_test.py`: Granger causality testing
- `main.py`: Main bot logic and scheduling

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License 