#!/bin/sh

echo "Workbook Directory : $1"
echo "Repo Token : $2"

cp -r /action/* /github/workspace/

python main.py --workbook_dir=$1 --repo_token=$2

exit_status=$?
if [ "${exit_status}" -ne 0 ];
then
    echo "exit ${exit_status}"
    exit 1
fi
# echo "EXIT 0"
exit 0
