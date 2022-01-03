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
from .database import trade_row_to_transaction


def in_date_range(filtered_dates):
    def is_date_in_range(date_check):
        for start_date, end_date in filtered_dates:
            if date_check >= start_date and date_check <= end_date:
                return True
        return False
    return is_date_in_range


WASHED_TRANSACTION_30_DAYS_BEFORE = relativedelta(days=-30)
WASHED_TRANSACTION_30_DAYS_AFTER = relativedelta(days=30)


def filter_transaction_by_dates(dates: str):

    filtered_dates = set()
    if dates:
        filtered_dates = {
            datetime.strptime(date_text.strip(), '%y%m%d').date() for date_text
            in dates.split(',')
        }
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


def report_in_text(result: dict):
    yield header_break
    yield header_format('Symbol', 'quantity', 'Strike', 'Expiration',
                       'Acquired', 'Sold', 'Cost', 'Proceed', 'Gain/Loss')
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
            total += transaction.proceed - transaction.cost
        total_summary += total
        yield entry_break
        yield '|' + ' ' * 90 + '| {:10} | {:10} |'.format('gain/loss', total)
        yield entry_break
    yield '|' + ' ' * 77 + '| {:>23} | {:10} |'.format('total gain/loss', total_summary)
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
WHERE account_type = "{}"
"""
    return query_with_filter.format(account_type).strip()


def new_dates_filter_sql(dates):
    if not dates:
        return ""
    option_dates = {
        datetime.strptime(date_text.strip(), '%y%m%d').date() for date_text
        in dates.split(',')}
    stock_sold_dates = {
        (date_entry + WASHED_TRANSACTION_30_DAYS_BEFORE,
         date_entry + WASHED_TRANSACTION_30_DAYS_AFTER) for date_entry in
        option_dates}
    option_date_filter_sql = 'expiration in ({})'.format(
        ", ".join(map(lambda x: f'date("{x}")', option_dates)))
    equity_date_filter_sql = "OR ".join(
        ['(sold_date BETWEEN {} AND {})'.format(
        f'date("{x[0]}")', f'date("{x[1]}")') for x in stock_sold_dates])
    return (f'({option_date_filter_sql}) OR '
            f'(equity_class="stock" AND ({equity_date_filter_sql}))')


def new_symbols_filter_sql(symbols):
    if not symbols:
        return ""
    symbol_filter_sql = "symbol IN ({})".format(
        ", ".join(map(lambda x: f'"{x.strip()}"', symbols.split(","))))
    return symbol_filter_sql


def new_where_sql(account_filter, symbol_filter, date_filter):
    where_clause = []
    if account_filter:
        where_clause.append(f'(account_number IN ({account_filter}))')
    if symbol_filter:
        where_clause.append(f'({symbol_filter})')
    if date_filter:
        where_clause.append(f'({date_filter})')
    if where_clause:
        return f" WHERE {' AND '.join(where_clause)}"
    return ""


SELECT_TRADE_SQL = 'SELECT * FROM trade'
TRADE_ORDER_SQL = ' ORDER BY symbol, equity_class, sold_date'

def _handle_db(cli_args):
    from .database import init_connection
    account_filter_sql = new_account_filter_sql(cli_args.account)
    date_filter_sql = new_dates_filter_sql(cli_args.dates)
    symbol_filter_sql = new_symbols_filter_sql(cli_args.symbols)
    #conn = init_connection(f'{cli_args.db_file}')
    where_sql = new_where_sql(
        account_filter_sql, symbol_filter_sql, date_filter_sql)
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
            '-symbols', action='store', help="comma separated symbols to filter")
        sub_parser.add_argument(
            '-account', action='store', default='joint',
            choices=('joint', 'single', 'both'), help='account type')
        sub_parser.add_argument(
            '-dates', action='store',
            help="comma separated date in the format of YYMMDD")
        sub_parser.add_argument(
            '-summary', action='store_true', help='generate a summary only')

    def build_args_parser():
        import argparse
        cli_parser = argparse.ArgumentParser(epilog='knowledge is power!')
        sub_parsers = cli_parser.add_subparsers(
            help='sub commands', dest='sub_command')
        csv_file_parser = sub_parsers.add_parser('csv', help='csv sub command')
        db_file_parser = sub_parsers.add_parser('db', help='db sub command')
        build_csv_sub_command_parser(csv_file_parser)
        build_db_sub_command_parser(db_file_parser)
        return cli_parser

    cli_parser = build_args_parser()
    cli_args = cli_parser.parse_args()
    _main_entrypoint(cli_args)
