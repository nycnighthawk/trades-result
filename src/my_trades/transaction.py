#!/bin/env python
from decimal import Decimal
from collections import defaultdict
from csv import reader
from dataclasses import dataclass
from datetime import datetime, date
from functools import reduce
from pathlib import Path
from dateutil.relativedelta import relativedelta


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
    holding: Stock | Call | Put
    cusip: str
    description: str
    quantity: int
    acquired_date: date
    sold_date: date
    cost: Decimal
    proceed: Decimal
    # short_term_gain_loss: Decimal
    # long_term_gain_loss: Decimal


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


def build_transaction(holding: Stock | Call | Put,
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
    quantity = int(csv_entry[QUANTITY].split('.')[0])
    return Transaction(
        holding, cusip, csv_entry[DESCRIPTION],
        quantity, acquired_date, sold_date, cost, proceed)
        # short_term_gain_loss, long_term_gain_loss)


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


def in_date_range(filtered_dates):
    def is_date_in_range(date_check):
        for start_date, end_date in filtered_dates:
            if date_check >= start_date and date_check <= end_date:
                return True
        return False
    return is_date_in_range


WASHED_TRANSACTION_30_DAYS_BEFORE = relativedelta(days=-30)
WASHED_TRANSACTION_30_DAYS_AFTER = relativedelta(days=30)


def filter_transaction_by_dates(filtered_dates):
    washed_transaction_periods = {
        (date_entry + WASHED_TRANSACTION_30_DAYS_BEFORE,
         date_entry + WASHED_TRANSACTION_30_DAYS_AFTER) for date_entry in
        filtered_dates
    }

    is_date_within_washed_sale_period = in_date_range(
        washed_transaction_periods)

    def transaction_in_filtered_date(transaction):
        if not filtered_dates:
            return True
        if isinstance(transaction.holding, Option):
            if transaction.holding.expiration in filtered_dates:
                return True
            return False
        if is_date_within_washed_sale_period(transaction.acquired_date) \
                or is_date_within_washed_sale_period(transaction.sold_date):
            return True
        return False

    return transaction_in_filtered_date


REPORT_DATE_FORMAT = '%m-%d-%Y'
ZERO = Decimal(0)


def extract_report_component(
    transaction: Transaction
) -> tuple:
    gain_loss = transaction.proceed - transaction.cost
    if isinstance(transaction.holding, Call):
        return (
            f'C: {transaction.holding.strike}',
            transaction.holding.expiration.strftime(REPORT_DATE_FORMAT),
            transaction.acquired_date.strftime(REPORT_DATE_FORMAT),
            transaction.sold_date.strftime(REPORT_DATE_FORMAT),
            gain_loss)
    elif isinstance(transaction.holding, Put):
        return (
            f'P: {transaction.holding.strike}',
            transaction.holding.expiration.strftime(REPORT_DATE_FORMAT),
            transaction.acquired_date.strftime(REPORT_DATE_FORMAT),
            transaction.sold_date.strftime(REPORT_DATE_FORMAT),
            gain_loss)
    return (
        '', '', transaction.acquired_date.strftime(REPORT_DATE_FORMAT),
        transaction.sold_date.strftime(REPORT_DATE_FORMAT),
        gain_loss
    )


entry_format = '| {:10} | {:<10} | {:10} | {:10} | {:10} | {:10} |'.format
header_break = '+' + '='* 77 + '+'
# entry_break = '+' + '-' * 77 + '+'
entry_break = '+' + ('-' * 12 + '+') * 5 + '-' * 12 + '+'


def report_in_text(result):
    yield header_break
    yield entry_format('Symbol', 'Strike', 'Expiration', 'Acquired',
                       'Sold', 'Gain/Loss')
    yield header_break
    for symbol, transactions in result.items():
        do_symbol_output = True
        total = Decimal(0)
        for transaction in transactions:
            if do_symbol_output:
                do_symbol_output = False
                yield entry_format(
                    symbol, *extract_report_component(transaction))
            else:
                yield entry_format('', *extract_report_component(transaction))
            total += transaction.proceed - transaction.cost
        yield entry_break
        yield '|' + ' ' * 51 + '| {:10} | {:10} |'.format('gain/loss', total)
        yield entry_break


def _main_entrypoint(cli_args):
    symbols = {
        symbol.strip().lower() for symbol in cli_args.symbols.split(',')}
    if cli_args.dates:
        filtered_dates = {
            datetime.strptime(date_text.strip(), '%y%m%d').date() for date_text
            in cli_args.dates.split(',')
        }
    else:
        filtered_dates = set()
    in_filtered_dates = filter_transaction_by_dates(filtered_dates)
    final_result = defaultdict(list)
    for transaction in csv_to_transactions(cli_args.file):
        if transaction.holding.symbol in symbols \
                and in_filtered_dates(transaction):
            final_result[transaction.holding.symbol].append(transaction)

    print("\n".join(report_in_text(final_result)))


if __name__ == '__main__':
    import argparse
    cli_parser = argparse.ArgumentParser(epilog='knowledge is power!')
    cli_parser.add_argument(
        '-file', action='store', required=True,
        help='csv gain lost file')
    cli_parser.add_argument(
        '-symbols', action='store', required=True,
        help="comma separated symbols to filter"
    )
    cli_parser.add_argument(
        '-dates', action='store',
        help="comma separated date in the format of YYMMDD"
    )
    cli_args = cli_parser.parse_args()
    _main_entrypoint(cli_args)
