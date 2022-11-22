#!/bin/env python
from decimal import Decimal
from collections import defaultdict
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from itertools import chain
from pathlib import Path
from typing import Iterable
from .record import Option, Call, Put, Transaction, csv_to_transactions
from .database import trade_row_to_transaction


def in_date_range(
    filtered_dates: Iterable[tuple[date | None, date | None] | date]
):
    date_ranges = [date_data for date_data in filtered_dates
                   if isinstance(date_data, tuple)]
    dates = [date_data for date_data in filtered_dates
             if not isinstance(date_data, tuple)]
    def is_date_in_range(date_being_checked):
        for start_date, end_date in date_ranges:
            if start_date is None and end_date is None:
                return False
            if start_date and date_being_checked >= start_date \
                    and not end_date:
                return True
            if end_date and date_being_checked <= end_date \
                    and not start_date:
                return True
            if date_being_checked >= start_date \
                    and date_being_checked <= end_date:
                return True
        for date_to_check in dates:
            if date_being_checked == date_to_check:
                return True
        return False
    return is_date_in_range


DEFAULT_DAY_RANGE = 30
DAYS_BEFORE = relativedelta(days=-DEFAULT_DAY_RANGE)
DAYS_AFTER = relativedelta(days=DEFAULT_DAY_RANGE)


def filter_transaction_by_dates(dates: str):

    filtered_dates = set()
    if dates:
        filtered_dates = {
            datetime.strptime(date_text.strip(), '%y%m%d').date() for date_text
            in dates.split(',')
        }
    transaction_period = {
        (date_entry + DAYS_BEFORE,
         date_entry + DAYS_AFTER) for date_entry in
        filtered_dates
    }

    is_date_within_period = in_date_range(
        transaction_period)

    def transaction_in_filtered_date(transaction):
        if not filtered_dates:
            return True
        if isinstance(transaction.holding, Option):
            if transaction.holding.expiration in filtered_dates:
                return True
            return False
        if is_date_within_period(transaction.acquired_date) \
                or is_date_within_period(transaction.sold_date):
            return True
        return False

    return transaction_in_filtered_date


def filter_transaction_by_symbols(symbols: str):

    filtered_symbols = set()

    if symbols:
        filtered_symbols = {
            symbol.strip().lower() for symbol in symbols.split(',')}

    def transaction_in_filtered_symbols(transaction: Transaction):
        if not filtered_symbols:
            return True
        if transaction.holding.symbol in filtered_symbols:
            return True
        return False

    return transaction_in_filtered_symbols


REPORT_DATE_FORMAT = '%m-%d-%Y'
ZERO = Decimal(0)

bool_to_yes_no = lambda v: "Y" if v else "N"


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
            gain_loss,
            bool_to_yes_no(transaction.wash_sale))
    elif isinstance(transaction.holding, Put):
        return (
            transaction.quantity,
            f'P: {transaction.holding.strike}',
            transaction.holding.expiration.strftime(REPORT_DATE_FORMAT),
            transaction.acquired_date.strftime(REPORT_DATE_FORMAT),
            transaction.sold_date.strftime(REPORT_DATE_FORMAT),
            transaction.cost,
            transaction.proceed,
            gain_loss,
            bool_to_yes_no(transaction.wash_sale))
    return (
        transaction.quantity, '', '',
        transaction.acquired_date.strftime(REPORT_DATE_FORMAT),
        transaction.sold_date.strftime(REPORT_DATE_FORMAT),
        transaction.cost,
        transaction.proceed,
        gain_loss,
        bool_to_yes_no(transaction.wash_sale))


header_format = ('| {:10} | {:<10.2} | {:<10} | {:10} | '
                 '{:10} | {:10} | {:10} | {:10} | {:10} | {:9} |').format
entry_format = ('| {:10} | {:<10.2f} | {:<10} | {:10} | {:10} '
                '| {:10} | {:10} | {:10} | {:10} | {:9} |').format
header_break = '+' + '='* 128 + '+'
# entry_break = '+' + '-' * 103 + '+'
entry_break = ('+' + ('-' * 12 + '+') + ('-' * 12 + '+') * 7
               + '-' * 12 + '+' + '-' * 11 + '+')


def report_in_text(result: dict):
    yield header_break
    yield header_format(
        'Symbol', 'quantity', 'Strike', 'Expiration', 'Acquired', 'Sold',
        'Cost', 'Proceed', 'Gain/Loss', 'Wash Sale')
    yield header_break
    total_summary = Decimal(0)
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
            if not transaction.wash_sale:
                total += transaction.proceed - transaction.cost
        total_summary += total
        yield entry_break
        yield '|' + ' ' * 90 + '| {:10} | {:10} | {:9} |'.format('gain/loss', total, '')
        yield entry_break
    yield '|' + ' ' * 77 + '| {:>23} | {:10} | {:9} |'.format(
        'total gain/loss', total_summary, '')
    yield entry_break


def _handle_csv(cli_args):
    in_filtered_dates = filter_transaction_by_dates(cli_args.dates)
    in_filtered_symbols = filter_transaction_by_symbols(cli_args.symbols)
    final_result = defaultdict(list)
    for transaction in csv_to_transactions(cli_args.file, cli_args.account):
        if in_filtered_symbols(transaction) \
                and in_filtered_dates(transaction):
            final_result[transaction.holding.symbol].append(transaction)

    print("\n".join(report_in_text(final_result)))


def new_account_filter_sql(account_type: str):
    if account_type == 'both':
        query_both_account = \
"""
SELECT account_number FROM account
"""
        return query_both_account.strip()

    query_with_filter = \
"""
SELECT account_number FROM account
WHERE account_type = '{}'
"""
    return query_with_filter.format(account_type).strip()


SQL_DATE = "date('{}')".format


def parse_date(date_range: str):
    if '-' in date_range:
        start_date, end_date = date_range.split('-')
        start_date = datetime.strptime(start_date.strip(), '%y%m%d').date() \
            if start_date else None
        end_date = datetime.strptime(end_date.strip(), '%y%m%d').date() \
            if end_date else None
        return (start_date, end_date)
    return datetime.strptime(date_range.strip(), '%y%m%d').date()


def date_range_filter_sql(date_range: tuple[date | None, date | None]) -> str:
    start_date, end_date = date_range
    if start_date and end_date:
        return f'BETWEEN {SQL_DATE(start_date)} and {SQL_DATE(end_date)}'
    if start_date:
        return f'>= {SQL_DATE(start_date)}'
    return f'<= {SQL_DATE(end_date)}'


def _dates_filter_sql(
    field_name: str,
    dates: tuple[list[tuple[date | None, date | None]], list[date]]
) -> str:
    date_range_filter = (
        '({} {})'.format(field_name, date_range_filter_sql(date_range))
        for date_range in dates[0])
    date_filter = ('({} IN ({}))'.format(field_name,
        ", ".join(map(lambda x: SQL_DATE(x), dates[1]))),) if dates[1] \
        else tuple()
    return " OR ".join(chain(date_range_filter, date_filter))


def group_dates(
    dates: Iterable[tuple[date | None, date | None] | date]
) -> tuple[list[tuple[date | None, date | None]], list[date]]:
    date_ranges = []
    single_dates = []
    for date in dates:
        if isinstance(date, tuple):
            date_ranges.append(date)
        else:
            single_dates.append(date)
    return date_ranges, single_dates


def dates_filter_sql(field_name: str, dates: str) -> str:
    if not dates:
        return ""
    dates_group = group_dates(
        parse_date(date.strip()) for date in dates.split(','))
    return _dates_filter_sql(field_name, dates_group)


def new_symbols_filter_sql(symbols):
    if not symbols:
        return ""
    symbol_filter_sql = "symbol IN ({})".format(
        ", ".join(map(lambda x: f"'{x.strip()}'", symbols.split(","))))
    return symbol_filter_sql


def new_where_sql(
    account_filter, symbol_filter, expiration_filter,
    date_range_filter) -> str:
    where_clause = []
    if account_filter:
        where_clause.append(f'(account_number IN ({account_filter}))')
    if symbol_filter:
        where_clause.append(f'({symbol_filter})')
    if expiration_filter:
        where_clause.append(f'({expiration_filter})')
    if date_range_filter:
        where_clause.append(f'({date_range_filter})')
    if where_clause:
        return f" WHERE {' AND '.join(where_clause)}"
    return ""


SELECT_TRADE_SQL = 'SELECT * FROM trade'
TRADE_ORDER_SQL = ' ORDER BY symbol, equity_class, sold_date'

def _handle_db(cli_args):
    from .database import init_connection
    account_filter_sql = new_account_filter_sql(cli_args.account)
    expiration_filter_sql = dates_filter_sql(
        'expiration', cli_args.expiration)
    sold_date_sql = dates_filter_sql('sold_date', cli_args.dates)
    symbol_filter_sql = new_symbols_filter_sql(cli_args.symbols)
    #conn = init_connection(f'{cli_args.db_file}')
    where_sql = new_where_sql(
        account_filter_sql, symbol_filter_sql, expiration_filter_sql,
        sold_date_sql)
    query_sql = SELECT_TRADE_SQL + where_sql + TRADE_ORDER_SQL + ';'
    conn = init_connection(f'{cli_args.db_file}')
    transactions = defaultdict(list)
    print(query_sql)
    for row in conn.execute(query_sql):
        transaction = trade_row_to_transaction(row)
        transactions[transaction.holding.symbol].append(transaction)
    print("\n".join(report_in_text(transactions)))


def _main_entrypoint(cli_args):
    if cli_args.sub_command == 'db':
        return _handle_db(cli_args)
    return _handle_csv(cli_args)


if __name__ == '__main__':

    import sys

    def build_csv_sub_command_parser(sub_parser):
        sub_parser.add_argument(
            '-file', action='store', type=Path,
            help='csv gain lost file')
        sub_parser.add_argument(
            '-account', action='store', default='generic',
            help='account number')
        sub_parser.add_argument(
            '-symbols', action='store', default='',
            help="comma separated symbols to filter")
        sub_parser.add_argument(
            '-dates', action='store', default='',
            help="comma separated date in the format of YYMMDD")
        sub_parser.add_argument(
            '-summary', action='store_true', help='generate a summary only')

    def build_db_sub_command_parser(sub_parser):
        from .database import DEFAULT_DB_PATH
        sub_parser.add_argument(
            '-db_file', action='store', type=Path,
            default=DEFAULT_DB_PATH,
            help='db file')
        sub_parser.add_argument(
            '-symbols', action='store',
            help="comma separated symbols to filter")
        sub_parser.add_argument(
            '-account', action='store', default='both',
            choices=('joint', 'single', 'both'), help='account type')
        sub_parser.add_argument(
            '-expiration', action='store',
            help=('Expiration of the options using date format yymmdd. '
                  'Multiple option expirations can be separated by comma.'))
        sub_parser.add_argument(
            '-dates', action='store', default="",
            help=('start and end date separated by "-"'
                  'for example 210101-220101 represents date between '
                  '01/01/2021 and 01/01/2022. Multiple date ranges can be '
                  'separated by comma, date range can be open ended. For '
                  'example, 210101- means all date >= 01/01/2021 or '
                  '-220101 means all dates <= 01/01/2022. Single date can '
                  'be sepcify without "-". Comma can be used for multiple '
                  'dates'))
        sub_parser.add_argument(
            '-summary', action='store_true', help='generate a summary only')

    def build_args_parser():
        import argparse
        cli_parser = argparse.ArgumentParser(epilog='knowledge is power!')
        sub_parsers = cli_parser.add_subparsers(
            help='sub commands', dest='sub_command')
        csv_file_parser = sub_parsers.add_parser('csv', help='csv sub command')
        db_file_parser = sub_parsers.add_parser(
            'db', help='db sub command, default sub command')
        build_csv_sub_command_parser(csv_file_parser)
        build_db_sub_command_parser(db_file_parser)
        return cli_parser

    cli_parser = build_args_parser()
    if len(sys.argv) == 1:
        cli_args = cli_parser.parse_args(['db'])
    elif sys.argv[1] not in ('db', 'csv')\
            and sys.argv[1] not in ('-h', '--h', '--help') \
            and sys.argv[1].startswith('-'):
        cli_args = cli_parser.parse_args(['db'] + sys.argv[1:])
    else:
        cli_args = cli_parser.parse_args()
    _main_entrypoint(cli_args)
