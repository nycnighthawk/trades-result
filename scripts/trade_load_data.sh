#!/bin/bash

script_dir="$(dirname $(readlink -f ${BASH_SOURCE}))/.."

prog_name=$(basename $0)
_arg_del="0"

if [ "$1" = "--delete" ] || [ "$1" = "-d" ]
then
    _arg_del="1"
elif [ "$1" = "-h" ] || [ "$1" = "--help" ]
then
    cat <<- END
${prog_name}: load trading csv P&L data into the database
Usage:
    ${prog_name} [options]

Description:
    -h
    --help               Display the help
    -d
    --delete             Delete the csv file
END
fi

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
fi
if [ -f ${joint_account_file} ]
then
    python -m my_trades.database -account joint -file "${joint_account_file}"
fi

if [ "${_arg_del}" = "1" ]
then
    if [ -f ${single_account_file} ]
    then
        rm -f "${single_account_file}"
    fi
    if [ -f ${joint_account_file} ]
    then
        rm -f "${joint_account_file}"
    fi
fi
