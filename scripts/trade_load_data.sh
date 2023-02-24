#!/bin/bash

script_dir="$(dirname $(readlink -f ${BASH_SOURCE}))/.."

if [ -d "${script_dir}/venv" ]
then
    my_trades_venv="${script_dir}/venv"
elif [ -d "${script_dir}/.venv" ]
then
    my_trades_venv="${script_dir}/.venv"
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
