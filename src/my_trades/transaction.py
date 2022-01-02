#!/bin/env python
from decimal import Decimal
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date
from functools import reduce
from pathlib import Path
from dateutil.relativedelta import relativedelta
from .record import (
    Equity, Option, Call, Put, Stock, Transaction, csv_to_transactions)


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
            transaction.quantity,
            f'C: {transaction.holding.strike}',
            transaction.holding.expiration.strftime(REPORT_DATE_FORMAT),
            transaction.acquired_date.strftime(REPORT_DATE_FORMAT),
            transaction.sold_date.strftime(REPORT_DATE_FORMAT),
            transaction.cost,
            transaction.proceed,
            gain_loss)
    elif isinstance(transaction.holding, Put):
        return (
            transaction.quantity,
            f'P: {transaction.holding.strike}',
            transaction.holding.expiration.strftime(REPORT_DATE_FORMAT),
            transaction.acquired_date.strftime(REPORT_DATE_FORMAT),
            transaction.sold_date.strftime(REPORT_DATE_FORMAT),
            transaction.cost,
            transaction.proceed,
            gain_loss)
    return (
        transaction.quantity, '', '',
        transaction.acquired_date.strftime(REPORT_DATE_FORMAT),
        transaction.sold_date.strftime(REPORT_DATE_FORMAT),
        transaction.cost,
        transaction.proceed,
        gain_loss
    )


header_format = '| {:10} | {:<10.2} | {:<10} | {:10} | {:10} | {:10} | {:10} | {:10} | {:10} |'.format
entry_format = '| {:10} | {:<10.2f} | {:<10} | {:10} | {:10} | {:10} | {:10} | {:10} | {:10} |'.format
header_break = '+' + '='* 116 + '+'
# entry_break = '+' + '-' * 103 + '+'
entry_break = '+' + ('-' * 12 + '+') + ('-' * 12 + '+') * 7 + '-' * 12 + '+'


def report_in_text(result):
    yield header_break
    yield header_format('Symbol', 'quantity', 'Strike', 'Expiration',
                       'Acquired', 'Sold', 'Cost', 'Proceed', 'Gain/Loss')
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
        yield '|' + ' ' * 90 + '| {:10} | {:10} |'.format('gain/loss', total)
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
    for transaction in csv_to_transactions(cli_args.file, cli_args.account):
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
        '-account', action='store', default='generic',
        help='account number')
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
