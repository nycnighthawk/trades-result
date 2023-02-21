#!/bin/bash

if [ -d "${HOME}/projects/my_trades/venv" ]
then
    my_trades_venv="${HOME}/projects/my_trades/venv"
elif [ -d "${HOME}/projects/my_trades/.venv" ]
then
    my_trades_venv="${HOME}/projects/my_trades/.venv"
fi
. "${my_trades_venv}/bin/activate"

single_account_file=${HOME}/Downloads/Realized_Gain_Loss_Account_X69469547.csv
joint_account_file=${HOME}/Downloads/Realized_Gain_Loss_Account_X96392103.csv
if [ -f ${single_account_file} ]
then
    python -m my_trades.database -account single -file "${single_account_file}"
    rm -f "${single_account_file}"
fi
if [ -f ${joint_account_file} ]
then
    python -m my_trades.database -account joint -file "${joint_account_file}"
    rm -f "${joint_account_file}"
fi
