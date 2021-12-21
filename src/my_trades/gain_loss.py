#!/bin/env python

from collections import defaultdict
from decimal import Decimal
from functools import reduce
from .transaction import Transaction, csv_to_transactions, Option
from datetime import timedelta

ONE_YEAR = timedelta(days=365)

def calculate_long_term_gain_loss(
    accumulator: Decimal,
    transaction: Transaction
) -> Decimal:
    if isinstance(transaction.holding, Option):
        return accumulator
    if (transaction.sold_date - transaction.acquired_date) > ONE_YEAR:
        return accumulator + (transaction.proceed - transaction.cost)
    return accumulator


def calculate_short_term_gain_loss(
    accumulator: Decimal,
    transaction: Transaction
) -> Decimal:
    if isinstance(transaction.holding, Option):
        return accumulator + (transaction.proceed - transaction.cost)
    if (transaction.sold_date - transaction.acquired_date) < ONE_YEAR:
        return accumulator + (transaction.proceed - transaction.cost)
    return accumulator


def gain_loss(cumulative_gain_loss: tuple[Decimal, Decimal],
              transaction: Transaction) -> tuple[Decimal, Decimal]:
    return (
        calculate_short_term_gain_loss(
            cumulative_gain_loss[0], transaction),
        calculate_long_term_gain_loss(
            cumulative_gain_loss[1], transaction))


def filtered_gain_loss(
    symbols: tuple):
    def _filtered_gain_loss(
        cumulative_gain_loss: dict[str, tuple[Decimal, Decimal]],
        transaction: Transaction
    ) -> dict[str, tuple[Decimal, Decimal]]:
        if transaction.holding.symbol in symbols:
            short_term_gain_loss, long_term_gain_loss = (
                calculate_short_term_gain_loss(
                    cumulative_gain_loss[transaction.holding.symbol][0],
                    transaction),
                calculate_long_term_gain_loss(
                    cumulative_gain_loss[transaction.holding.symbol][1],
                    transaction))
            cumulative_gain_loss[transaction.holding.symbol] = (
                short_term_gain_loss, long_term_gain_loss)
        return cumulative_gain_loss

    return _filtered_gain_loss


def _main_entrypoint(cli_args):

    transactions = tuple(csv_to_transactions(cli_args.file))

    if cli_args.symbols:
        symbols = {
            symbol.strip().lower() for symbol in cli_args.symbols.split(",")}
        filtered_result = reduce(
            filtered_gain_loss(symbols),
            transactions,
            defaultdict(lambda: (Decimal(0), Decimal(0))))
        for key, value in filtered_result.items():
            print(f'              Symbol: {key}')
            print(f'short term gain/loss: {value[0]}')
            print(f' long term gain/loss: {value[1]}')
            print(f'     total gain/loss: {value[0] + value[1]}')
            print('-' * 40)
    print('Summary:')
    short_term_gain_loss, long_term_gain_loss = \
        reduce(gain_loss, transactions,
               (Decimal(0), Decimal(0)))
    print(f'short term gain/loss: {short_term_gain_loss}')
    print(f' long term gain/loss: {long_term_gain_loss}')
    print(f'     total gain/loss: '
          f'{short_term_gain_loss + long_term_gain_loss}')


if __name__ == '__main__':
    import argparse

    cli_parser = argparse.ArgumentParser(epilog='knowledge is power!')
    cli_parser.add_argument(
        '-file', action='store', required=True,
        help='csv gain lost file')
    cli_parser.add_argument(
        '-symbols', action='store',
        help="comma separated symbols to filter"
    )
    cli_args = cli_parser.parse_args()
    _main_entrypoint(cli_args)
