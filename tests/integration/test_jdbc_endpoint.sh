#!/bin/bash
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

POD_NAME=$1
NAMESPACE=${2:-default}
JDBC_ENDPOINT=$3
DB_NAME=${4:-testdb}
TABLE_NAME=${5:-testtable}
USERNAME=${6:-}
PASSWORD=${7:-}
SQL_COMMANDS=$(cat ./tests/integration/setup/test.sql | sed "s/db_name/$DB_NAME/g" | sed "s/table_name/$TABLE_NAME/g")


if [ -z "${USERNAME}" ]; then
    echo -e "$(kubectl exec $POD_NAME -n $NAMESPACE -- \
            env CMDS="$SQL_COMMANDS" ENDPOINT="$JDBC_ENDPOINT" \
            /bin/bash -c 'echo "$CMDS" | /opt/kyuubi/bin/beeline -u \"$ENDPOINT\"'
        )" > /tmp/test_beeline.out
else 
    echo -e "$(kubectl exec $POD_NAME -n $NAMESPACE -- \
            env CMDS="$SQL_COMMANDS" ENDPOINT="$JDBC_ENDPOINT" USER="$USERNAME" PASSWD="$PASSWORD"\
            /bin/bash -c 'echo "$CMDS" | /opt/kyuubi/bin/beeline -u $ENDPOINT -n $USER -p $PASSWD'
        )" > /tmp/test_beeline.out
fi


num_rows_inserted=$(cat /tmp/test_beeline.out | grep "Inserted Rows:" | sed 's/|/ /g' | tail -n 1 | xargs | rev | cut -d' ' -f1 | rev )
echo -e "${num_rows_inserted} rows were inserted."

if [ "${num_rows_inserted}" != "3" ]; then
    echo "ERROR: Test failed. ${num_rows_inserted} out of 3 rows were inserted. Aborting with exit code 1."
    exit 1
fi

rm /tmp/test_beeline.out
