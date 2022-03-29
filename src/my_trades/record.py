#!/usr/bin/env python
from collections import defaultdict
from csv import reader
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import count
from operator import attrgetter
from pathlib import Path
from uuid import uuid3, NAMESPACE_URL
from .logger import logger


SYMBOL = 0
DESCRIPTION = 1
QUANTITY = 2
ACQUIRED_DATE = 3
SOLD_DATE = 4
PROCEED = 5
COST = 6
# SHORT_TERM_GAIN_LOSS = 7
# LONG_TERM_GAIN_LOSS = 8


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
    account_number: str
    holding: Stock | Call | Put
    cusip: str
    description: str
    quantity: Decimal
    acquired_date: date
    sold_date: date
    cost: Decimal
    proceed: Decimal
    transaction_id: str = ''

    def __post_init__(self):
        if self.transaction_id == '':
            self.transaction_id = f'{new_transaction_id(self)}'

    def __hash__(self):
        return hash(self.cusip)

    def __getitem__(self, key):
        return self.__dict__.get(key)


def new_transaction_id():
    registry = defaultdict(lambda: count(1))

    transaction_fields = attrgetter(
        'account_number', 'cusip', 'acquired_date', 'sold_date', 'quantity',
        'cost', 'proceed')
    def generate_id(transaction: Transaction):
        key = '{}-{}-{}-{}-{:.2f}-{:.2f}-{:.2f}'.format(
            *transaction_fields(transaction))
        sequence_id = f'{next(registry[key]):02d}'
        return uuid3(NAMESPACE_URL, f'{key}-{sequence_id}')

    return generate_id


new_transaction_id = new_transaction_id()


def convert_currency(value):
    if value[0] == '$':
        return Decimal(value[1:])
    if value[0] == '(':
        return -Decimal(value[2:-1])
    if not value or value == '-':
        return Decimal(0)
    raise ValueError(f'invalid currency: {value}!')


def extract_option_date(value):
    return datetime.strptime(value, '%y%m%d').date()


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


def build_transaction(account_number: str,
                      holding: Stock | Call | Put,
                      cusip: str,
                      csv_entry):
    acquired_date = datetime.strptime(
        csv_entry[ACQUIRED_DATE], DATE_FORMAT).date()
    sold_date = datetime.strptime(
        csv_entry[SOLD_DATE], DATE_FORMAT).date()
    proceed = convert_currency(csv_entry[PROCEED].rstrip())
    cost = convert_currency(csv_entry[COST].rstrip())
    # short_term_gain_loss = convert_currency(csv_entry[SHORT_TERM_GAIN_LOSS])
    # long_term_gain_loss = convert_currency(csv_entry[LONG_TERM_GAIN_LOSS])
    quantity = Decimal(csv_entry[QUANTITY])
    return Transaction(
        account_number, holding, cusip, csv_entry[DESCRIPTION],
        quantity, acquired_date, sold_date, cost, proceed)
        # short_term_gain_loss, long_term_gain_loss)


def csv_entry_to_transaction(csv_entry, account_number: str):
    try:
        symbol, cusip, expiration, option_type, strike = extract_symbol(
            csv_entry[SYMBOL])

        if option_type is None:
            holding = Stock(symbol)
        elif option_type == 'p':
            holding = Put(symbol, strike, expiration)
        elif option_type == 'c':
            holding = Call(symbol, strike, expiration)
        return build_transaction(account_number, holding, cusip, csv_entry)
    except ValueError:
        logger.warning(f'{csv_entry} not processed!')
        raise


def csv_to_transactions(csv_file: str,
                        account_number: str = 'generic'):
    with Path(csv_file).expanduser().open('r') as text_stream:
        csv_reader = reader(text_stream)
        next(csv_reader)
        for entry in csv_reader:
            try:
                yield csv_entry_to_transaction(entry, account_number)
            except ValueError:
                continue
