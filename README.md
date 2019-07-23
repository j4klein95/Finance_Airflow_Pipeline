# **US Financial Data** 
## AAII Investor Sentiment Data (Source: Quandl Free API)
    * Date: Timestamp (Year-Month-Day, Must Have) 
    * Bullish: Double - (nullable) - representation of positive sentiment on stock market.
    * Neutral: Double - (nullable) - representation of neutral sentiment on stock market.
    * Bearish: Double - (nullable) - representation of negative sentiment on stock market.
    * Total: Double - (nullable, should be 1 on non-null rows) - aggregated sentiments.
    * Bullish 8-week Moving Average: Double - (nullable) positive sentiment moving average.
    * Bull-Bear Spread: Double - (nullable) - Absolute value of the difference between bullish and bearish sentiments.
    * Bullish Average: Double - (nullable) - average of bullish sentiments.
    * Bullish Average + Standard Deviation: Double - (nullable) - standard deviation added to buillish average. 
    * Buillish Average - Standard Deviation: Double - (nullable) - standard deviation subtracted from the bullish average. 
    * S&P 500 Weekly High: Double - (nullable) - The S&P 500 weekly high price. 
    * S&P 500 Weekly Low: Double - (nullable) - The S&P 500 weekly low price. 
    * S&P 500 Weekly Close: Double - (nullable) - The S&P 500 weekly closing price. 
    
## Monthly S&P 500 Composite Data (Source: Yale Department of Economics, Quandl Free API)
    * Date: Timestamp - (Year-Month-Day, non-nullable).
    * S&P Composite: Double - (non-nullable) - Price for S&P 500 Composite.
    * Dividend: Double - (nullable, specifically for most recent months).
    * Earnings: Double - (nullable for most recent months).
    * CPI: Double - (nullable) - Consumer Price Index.
    * Long Interest Rate - (nullable) - Long Interest Rate.
    * Real Price: Double - (non-nullable).
    * Real Dividenc: Double - (nullable).
    * Real Earnings: Double - (nullable).
    * Cyclically Adjusted PE Ratio: Double - (non-nullable).
    
## US Stock Market Confidence Indices, Individual (Source: Yale Department of Economics, Quandl Free API)
    * Date: Timestamp - (Year-Month-Day, non-nullable).
    * Valuation_Indices: Double - (nullable) - Confidence in Stock Market. 
    * Valuation_indices_Std_Err: Double - (nullable) - Standard error of Confidence Indices.
    * Crash_confidence: Double - (nullable) - Crash Confidence in Stock Market.
    * Crash_confidence_Std_Err: Double - (nullable) - CC standard error.
    * Buy_on_Dips: Double - (nullable) - Confidence to buy during dips.
    * Buy_on_Dips_Std_Err: Double - (nullable) - Standard error of BoD. 
    
## Historical Single Day Market Data (Source: World Trading Data)
    * Symbol: String - (non-nullable) - Ticker Symbol.
    * Date: Timestamp - (Year-Month-Day, non-nullable).
    * Open: Double - (nullable) - Opening Price.
    * Close: Double - (nullable) - Closing (EOD) Price.
    * High: Double - (nullable) - High price for that day. 
    * low: Double - (nullable) - lowest trading price for that day.
    * volume: Double - (nullable) - number shares that changed hands during the day. 