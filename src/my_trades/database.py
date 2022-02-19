#!/usr/bin/env python


from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import tee
from pathlib import Path
import sqlite3
from typing import Iterable, Callable
from .record import (
    Transaction, Stock, Option, Call, Put, csv_to_transactions)


DB_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE account_type (
    account_type TEXT NOT NULL,
    PRIMARY KEY (account_type));

CREATE TABLE account (
    account_number TEXT NOT NULL,
    account_type TEXT NOT NULL,
    PRIMARY KEY (account_number)
    FOREIGN KEY (account_type) REFERENCES account_type(account_type)
        ON DELETE CASCADE ON UPDATE CASCADE);

CREATE TABLE equity_class (
    equity_class TEXT NOT NULL,
    PRIMARY KEY (equity_class)
);

CREATE TABLE symbol (
    symbol TEXT NOT NULL,
    description TEXT DEFAULT "" NOT NULL,
    PRIMARY KEY (symbol)
);

CREATE TABLE trade (
    transaction_id TEXT NOT NULL,
    cusip TEXT NOT NULL,
    symbol TEXT NOT NULL,
    account_number TEXT NOT NULL,
    equity_class TEXT NOT NULL,
    strike INTEGER DEFAULT 0 NOT NULL,
    quantity INTEGER NOT NULL,
    expiration DATE DEFAULT NULL,
    acquired_date DATE NOT NULL,
    sold_date DATE NOT NULL,
    cost INTEGER NOT NULL,
    proceed INTEGER NOT NULL,
    description TEXT DEFAULT "" NOT NULL,
    PRIMARY KEY (transaction_id),
    FOREIGN KEY (symbol) REFERENCES symbol(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (equity_class) REFERENCES equity_class(equity_class) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (account_number) REFERENCES account(account_number) ON DELETE CASCADE ON UPDATE CASCADE
);

-- some initial data
INSERT INTO equity_class (equity_class)
VALUES
    ('stock'),
    ('call'),
    ('put');

INSERT INTO account_type (account_type)
VALUES
    ('single'),
    ('joint');
"""


def init_connection(file: str = ':memory:'):
    need_schema = False
    if file == ':memory:':
        connection = sqlite3.connect(
            file,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        need_schema = True
    else:
        file_path = Path(file).expanduser().resolve()
        if not file_path.exists():
            need_schema = True
        connection = sqlite3.connect(
            file_path.as_uri(), uri=True,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    if need_schema:
        create_schema(connection)
    connection.row_factory = sqlite3.Row
    return connection


def create_schema(connection: sqlite3.Connection):
    connection.executescript(DB_SCHEMA_SQL)
    connection.commit()


@dataclass
class Trade:
    account_number: str
    transaction_id: str
    cusip: str
    symbol: str
    equity_class: str
    strike: int
    quantity: int
    expiration: date | None
    acquired_date: date
    sold_date: date
    cost: int
    proceed: int
    description: str


STOCK_CLASS = 'stock'
CALL_CLASS = 'call'
PUT_CLASS = 'put'

def stock_to_trade(stock: Transaction):
    return Trade(stock.account_number, stock.transaction_id, stock.cusip,
                 stock.holding.symbol, STOCK_CLASS, 0,
                 int(stock.quantity * 100), None, stock.acquired_date,
                 stock.sold_date, int(stock.cost * 100),
                 int(stock.proceed * 100), stock.description)


def option_to_trade(option: Transaction) -> Trade:
    option_type = CALL_CLASS
    if isinstance(option.holding, Put):
        option_type = PUT_CLASS
    return Trade(option.account_number, option.transaction_id, option.cusip,
                 option.holding.symbol, option_type,
                 int(option.holding.strike * 100), int(option.quantity * 100),
                 option.holding.expiration, option.acquired_date,
                 option.sold_date, int(option.cost *  100),
                 int(option.proceed * 100), option.description)


def _to_decimal(value):
    return Decimal(value) / 100


def trade_to_transaction(trade: Trade) -> Transaction:
    if trade.equity_class == STOCK_CLASS:
        holding = Stock(trade.symbol)
    elif trade.equity_class == CALL_CLASS:
        holding = Call(trade.symbol, Decimal(trade.strike) / 100,
                       trade.expiration)
    else:
        holding = Put(trade.symbol, Decimal(trade.strike) / 100,
                      trade.expiration)
    return Transaction(trade.account_number, holding, trade.cusip,
                       trade.description, _to_decimal(trade.quantity),
                       trade.acquired_date, trade.sold_date,
                       _to_decimal(trade.cost), _to_decimal(trade.proceed),
                       trade.transaction_id)


def _to_holding(row: sqlite3.Row) -> Stock | Call | Put:
    equity_class = row['equity_class']
    if equity_class == STOCK_CLASS:
        return Stock(row['symbol'])
    if equity_class == CALL_CLASS:
        return Call(row['symbol'], _to_decimal(row['strike']),
            row['expiration'])
    return Put(row['symbol'], _to_decimal(row['strike']), row['expiration'])


def trade_row_to_transaction(row: sqlite3.Row) -> Transaction:
    holding = _to_holding(row)
    return Transaction(
        row['account_number'], holding, row['cusip'], row['description'],
        _to_decimal(row['quantity']),
        row['acquired_date'],
        row['sold_date'],
        _to_decimal(row['cost']),
        _to_decimal(row['proceed']), row['transaction_id'])


def trade_row_to_trade(row: sqlite3.Row) -> Trade:
    return Trade(
        row['account_number'], row['transaction_id'], row['cusip'],
        row['symbol'], row['equity_class'], row['strike'],
        row['quantity'],
        row['expiration'],
        row['acquired_date'],
        row['sold_date'],
        row['cost'], row['proceed'], row['description'])


def transactions_to_trades(
    transactions: Iterable[Transaction]
) -> Iterable[Trade]:

    def trade_from_transaction(transaction: Transaction):
        if isinstance(transaction.holding, Stock):
            return stock_to_trade(transaction)
        return option_to_trade(transaction)

    for transaction in transactions:
        yield trade_from_transaction(transaction)


INSERT_TRADE_SQL = """
INSERT INTO trade (
    transaction_id, cusip, symbol, account_number, equity_class, strike,
    quantity, expiration, acquired_date, sold_date,
    cost, proceed, description)
VALUES
    (?,?,?,?,?,?,?,?,?,?,?,?,?);
"""
INSERT_ACCOUNT_SQL = """
INSERT INTO account (account_number, account_type)
VALUES (?,?);
"""
INSERT_SYMBOL_SQL = """
INSERT INTO symbol (symbol)
VALUES (?);
"""

GET_TRANSACTION_ID_SQL = """
SELECT transaction_id FROM trade;
"""
GET_ACCOUNT_SQL = """
SELECT account_number FROM account;
"""
GET_SYMBOL_SQL = """
SELECT symbol FROM symbol;
"""


def insert_transactions(
    transactions: Iterable[Transaction],
    account_type: str
) -> Callable[[sqlite3.Connection], None]:

    trades = transactions_to_trades(transactions)
    def prepare_trades(trades: Iterable[Trade]) -> Iterable[tuple]:
        for trade in trades:
            yield (trade.transaction_id,
                   trade.cusip, trade.symbol, trade.account_number,
                   trade.equity_class, trade.strike, trade.quantity,
                   trade.expiration, trade.acquired_date,
                   trade.sold_date, trade.cost, trade.proceed,
                   trade.description)

    def prepare_account(accounts: Iterable[str]) -> Iterable[tuple]:
        for account in accounts:
            yield (account, account_type)

    def prepare_symbol(symbols: Iterable[str]) -> Iterable[tuple]:
        for symbol in symbols:
            yield (symbol,)

    def insert_account_to_db(trades: Iterable[Trade],
                             connection: sqlite3.Connection) -> None:
        account_numbers = {account_number[0] for account_number
                           in connection.execute(GET_ACCOUNT_SQL)}
        new_accounts = {t.account_number for t in trades
                        if t.account_number not in account_numbers}
        if new_accounts:
            connection.executemany(
                INSERT_ACCOUNT_SQL, prepare_account(new_accounts))
            connection.commit()

    def insert_symbol_to_db(trades: Iterable[Trade],
                            connection: sqlite3.Connection) -> None:
        existing_symbols = {symbol[0] for symbol in
                   connection.execute(GET_SYMBOL_SQL)}
        new_symbols = {t.symbol for t in trades
                       if t.symbol not in existing_symbols}
        if new_symbols:
            connection.executemany(
                INSERT_SYMBOL_SQL, prepare_symbol(new_symbols))
            connection.commit()

    def insert_to_db(connection: sqlite3.Connection) -> None:
        existing_transaction_ids = {
            transaction_id[0] for transaction_id
            in connection.execute(GET_TRANSACTION_ID_SQL)}
        trades_not_in_db = filter(
            lambda x: x.transaction_id not in existing_transaction_ids, trades)
        trades_not_in_db, account_processing, symbol_processing = \
            tee(trades_not_in_db, 3)
        insert_account_to_db(account_processing, connection)
        insert_symbol_to_db(symbol_processing, connection)
        connection.executemany(
            INSERT_TRADE_SQL, prepare_trades(trades_not_in_db))
        connection.commit()

    return insert_to_db


def _main_entrypoint(cli_args):
    conn = init_connection(f'{cli_args.db.resolve()}')
    if cli_args.account_number == 'generic':
        account_number = f'{cli_args.file.resolve().name}'.lower() \
            .split('.csv')[0].split('_')[-1].upper()
    else:
        account_number = cli_args.account_number
    transactions = csv_to_transactions(f'{cli_args.file}', account_number)
    insert_transactions(transactions, cli_args.account)(conn)


DEFAULT_DB_FILE = 'trades.db'
DEFAULT_DB_PATH = (Path(__file__).expanduser() \
                   / '../../../playground').resolve() / DEFAULT_DB_FILE

if __name__ == '__main__':
    import argparse
    cli_parser = argparse.ArgumentParser(epilog='knowledge is power!')
    cli_parser.add_argument(
        '-file', action='store', required=True,
        type=Path,
        help='csv gain lost file')
    cli_parser.add_argument(
        '-account_number', action='store', default='generic',
        help='account number')
    cli_parser.add_argument(
        '-account', action='store', default='joint',
        choices=('joint', 'single'),
        help="type of account, default to 'joint'")
    cli_parser.add_argument(
        '-db', action='store',
        default=DEFAULT_DB_PATH,
        type=Path,
        help='db file')

    cli_args = cli_parser.parse_args()

    _main_entrypoint(cli_args)
