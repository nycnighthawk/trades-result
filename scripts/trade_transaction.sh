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
python -m my_trades.transaction "$@"
