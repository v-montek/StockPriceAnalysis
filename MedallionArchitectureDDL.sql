IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
    PRINT 'Schema "bronze" created successfully.';
END


IF OBJECT_ID('bronze.stocks') IS NULL
BEGIN
	CREATE TABLE bronze.stocks(
	Symbol VARCHAR(10) NOT NULL,
	UnixTimestamp BIGINT NOT NULL,
	OpeningPriceForOneHourWindow Decimal(10,2) NOT NULL,
	ClosingPriceForOneHourWindow Decimal(10,2) NOT NULL,
	HighestPriceForOneHourWindow Decimal(10,2) NOT NULL,
	LowestPriceForOneHourWindow Decimal(10,2) NOT NULL,
	VolumeTradedForOneHourWindow Decimal(10,2) NOT NULL,
	TransactionsForOneHourWindow INT NOT NULL,
	Name VARCHAR(100), 
	Market VARCHAR(20), 
	PrimaryExchange  VARCHAR(20), 
	CIK VARCHAR(20), 
	CurrencyName VARCHAR(20)
	)
END

IF OBJECT_ID('bronze.stocks_staging') IS NULL
BEGIN
	CREATE TABLE bronze.stocks(
	Symbol VARCHAR(10) NOT NULL,
	UnixTimestamp BIGINT NOT NULL,
	OpeningPriceForOneHourWindow Decimal(10,2) NOT NULL,
	ClosingPriceForOneHourWindow Decimal(10,2) NOT NULL,
	HighestPriceForOneHourWindow Decimal(10,2) NOT NULL,
	LowestPriceForOneHourWindow Decimal(10,2) NOT NULL,
	VolumeTradedForOneHourWindow Decimal(10,2) NOT NULL,
	TransactionsForOneHourWindow INT NOT NULL,
	Name VARCHAR(100), 
	Market VARCHAR(20), 
	PrimaryExchange  VARCHAR(20), 
	CIK VARCHAR(20), 
	CurrencyName VARCHAR(20)
	)
END

IF OBJECT_ID('bronze.symbolReference') IS NULL
BEGIN
	CREATE TABLE bronze.symbolReference(
	Symbol VARCHAR(10) NOT NULL,
	LastProcessedDate DATE NULL,
	IsActive INT
	)
END