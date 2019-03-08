from runners.config import (
    BASELINE_METADATA_TABLE,
    DATA_SCHEMA_NAME,
)

from runners.helpers import db, log

from rpy2 import robjects as ro
from rpy2.robjects import pandas2ri

import json
import pandas

#  This function is going to be modified; instead of reading a metadata table, we're going to query tables that adhere to a naming standard and parse
#  comments on those tables to get metadata.

def read_metadata_table():
    query = f"""
        SELECT * from SNOWALERT.{DATA_SCHEMA_NAME}.{BASELINE_METADATA_TABLE};
        """

    try:
        ctx, rows = db.connect_and_fetchall(query)
        return ctx, rows
    except Exception as e:
        log.error("Unable to read metadata table: ", e)
        return None, None


def format_code(r, vars):
    ret = r
    for k, v in vars.items():
        ret = ret.replace(k, v)

    return ret


def pack(data):
    f = {}
    for row in data:
        if len(f) == 0:
            for k in row.keys():
                f.update({k: []})

        for k in row:
            f[k].append(row[k])

    return f


def unpack(data):
    b = [[data[k][i] for i in data[k]] for k in data]
    output = list(zip(*b))
    return output


def query_log_source(ctx, source):
    query = f"""SELECT * from {source};"""
    if ctx is not None:
        try:
            data = db.fetch(ctx, query)
        except Exception as e:
            log.error("Failed to query log source: ", e)
    else:
        log.error("No connection passed in for log source query.")
        return None

    f = pack(data)

    frame = pandas.DataFrame(f)
    pandas2ri.activate()
    r_dataframe = pandas2ri.py2rpy(frame)
    return r_dataframe


def check_output_table_query(table, results):
    columns = []
    for k in results:
        columns.append(k)

    column_names = " VARCHAR, ".join(columns)
    query = f"CREATE TABLE IF NOT EXISTS {table} ( " + column_names + " VARCHAR )"

    return query


def log_results_query(target, results):

    query = f"""insert into {target} values """
    for i in results:
        query += str(i) + ", "

    query = query[:-2]
    return query


def run_baseline(ctx, row):
    log_source = row[0]
    required_values = json.loads(row[1])
    output_table = row[2]
    code_location = row[3]

    with open(f"../baseline_modules/{code_location}/{code_location}.r", "r") as f:
        r_code = f.read()

    r_code = format_code(r_code, required_values)
    frame = query_log_source(ctx, log_source)
    ro.globalenv['input_table'] = frame

    output = ro.r(r_code).to_dict()
    try:
        db.execute(ctx, check_output_table_query(output_table, output))
    except Exception as e:
        log.error("Failed to create the output table", e)

    results = unpack(output)
    try:
        db.execute(ctx, log_results_query(output_table, results))
    except Exception as e:
        log.error("Failed to insert the results into the target table", e)


def main():
    ctx, rows = read_metadata_table()
    for row in rows:
        run_baseline(ctx, row)
