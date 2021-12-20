#!/bin/env python
from decimal import Decimal
from collections import defaultdict
from csv import reader
from dataclasses import dataclass
from datetime import datetime, date
from functools import reduce
from pathlib import Path


SYMBOL = 0
DESCRIPTION = 1
QUANTITY = 2
ACQUIRED_DATE = 3
SOLD_DATE = 4
PROCEED = 5
COST = 6
SHORT_TERM_GAIN_LOSS = 7
LONG_TERM_GAIN_LOSS = 8

@dataclass
class Equity:
    symbol: str


@dataclass
class Option(Equity):
    strike: Decimal
    expiration: date


class Call(Option):
    pass


class Put(Option):
    pass


class Stock(Equity):
    pass


@dataclass
class Transaction:
    holding: Stock | Call | Put
    cusip: str
    description: str
    quantity: int
    acquired_date: date
    sold_date: date
    cost: Decimal
    proceed: Decimal
    short_term_gain_loss: Decimal
    long_term_gain_loss: Decimal


def convert_currency(value):
    if value[0] == '$':
        return Decimal(value[1:])
    if value[0] == '(':
        return -Decimal(value[2:-1])
    if not value or value == '-':
        return Decimal(0)
    raise ValueError(f'invalid currency: {value}!')


def extract_option_date(value):
    return datetime.strptime(value, '%y%m%d')

OPTION_TYPES = 'cpCP'


def extract_symbol(value):
    cusip_start_pos = value.index('(')
    symbol_raw = value[:cusip_start_pos]
    cusip = value[cusip_start_pos+1:-1]
    digit_start = -1
    for n, c in enumerate(symbol_raw):
        if c.isdigit():
            digit_start = n
            break
    if digit_start == -1:
        return symbol_raw.lower(), cusip, None, None, None
    if digit_start and symbol_raw[digit_start + 6] in OPTION_TYPES:
        return (symbol_raw[:digit_start].lower(), cusip,
                extract_option_date(symbol_raw[digit_start:digit_start+6]),
                symbol_raw[digit_start + 6].lower(),
                Decimal(symbol_raw[digit_start + 7:]))
    for n in range(digit_start + 7, len(symbol_raw) + 1):
        if symbol_raw[n] in OPTIONS_TYPES:
            return (symbol_raw[:n-6].lower(), cusip,
                    extract_option_date(symbol_raw[n-6:n]),
                    symbol_raw[n].lower(),
                    Decimal(symbol_raw[n+1:]))
    raise ValueError(f'invalid symbol: {value}!')


DATE_FORMAT = '%m/%d/%Y'


def build_transaction(holding: Stock | Call | Put,
                      cusip: str,
                      csv_entry):
    acquired_date = datetime.strptime(csv_entry[ACQUIRED_DATE], DATE_FORMAT)
    sold_date = datetime.strptime(csv_entry[SOLD_DATE], DATE_FORMAT)
    proceed = convert_currency(csv_entry[PROCEED].rstrip())
    cost = convert_currency(csv_entry[COST].rstrip())
    short_term_gain_loss = convert_currency(csv_entry[SHORT_TERM_GAIN_LOSS])
    long_term_gain_loss = convert_currency(csv_entry[LONG_TERM_GAIN_LOSS])
    quantity = int(csv_entry[QUANTITY].split('.')[0])
    return Transaction(
        holding, cusip, csv_entry[DESCRIPTION],
        quantity, acquired_date, sold_date, cost, proceed,
        short_term_gain_loss, long_term_gain_loss)


def csv_entry_to_transaction(csv_entry):
    symbol, cusip, expiration, option_type, strike = extract_symbol(
        csv_entry[SYMBOL])

    if option_type is None:
        holding = Stock(symbol)
    elif option_type == 'p':
        holding = Put(symbol, strike, expiration)
    elif option_type == 'c':
        holding = Call(symbol, strike, expiration)
    return build_transaction(holding, cusip, csv_entry)


def csv_to_transactions(csv_file: str):
    with Path(csv_file).open('r') as text_stream:
        csv_reader = reader(text_stream)
        next(csv_reader)
        yield from (
            csv_entry_to_transaction(entry) for entry in csv_reader)
