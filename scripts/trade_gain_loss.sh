#!/bin/bash
if [ -d "${HOME}/projects/my_trades/venv" ]
then
    my_trades_venv="${HOME}/projects/my_trades/venv"
elif [ -d "${HOME}/projects/my_trades/.venv" ]
then
    my_trades_venv="${HOME}/projects/my_trades/.venv"
fi
. "${my_trades_venv}/bin/activate"
python -m my_trades.gain_loss "$@"
