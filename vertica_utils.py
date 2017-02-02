import vertica_python


class RowCountInValid(Exception):
    pass


class ErrorInUpdatingRowCountTableEntry(Exception):
    pass


def table_exists(cursor, table_name, schema_name):
    sql = ('SELECT * FROM tables WHERE table_name=\'' + table_name + '\' and table_schema=\'' + schema_name + '\';')
    try:
        cursor.execute(sql)
        return False if not cursor.fetchall() else True
    except vertica_python.errors.QueryError as err:
        raise err


def view_exists(cursor, table_name, schema_name):
    sql = ('SELECT * FROM views WHERE table_name=\'' + table_name + '\' and table_schema=\'' + schema_name + '\';')
    try:
        cursor.execute(sql)
        return False if not cursor.fetchall() else True
    except vertica_python.errors.QueryError as err:
        raise err


def table_or_view_exists(cursor, table_name, schema_name):
    return table_exists(cursor, table_name, schema_name) or view_exists(cursor, table_name, schema_name)


def get_row_count(cursor, table_name, schema_name, filter=';'):
    sql = ('SELECT COUNT(*) FROM ' + schema_name + '.' + table_name + filter)
    if table_or_view_exists(cursor, table_name, schema_name):
        try:
            cursor.execute(sql)
            row_cnt = cursor.fetchall()
            row_cnt = [-1] if not row_cnt else row_cnt[0]
            return row_cnt
        except:
            raise
    return [-1]


def get_row_count_by_filter(cursor, filter, table_name, schema_name):
    return get_row_count(cursor, table_name, schema_name, filter)


def rename_table(cursor, old_name, new_name, schema_name):
    sql = ('ALTER TABLE ' + schema_name + '.' + old_name + ' RENAME TO ' + new_name + ';')
    if table_or_view_exists(cursor, old_name, schema_name):
        try:
            cursor.execute(sql)
        except:
            raise


def drop_table(cursor, table_name, schema_name):
    sql = ('DROP TABLE IF EXISTS ' + schema_name + '.' + table_name)
    try:
        cursor.execute(sql)
    except:
        raise


def truncate_table(cursor, table_name, schema_name):
    sql = ('TRUNCATE TABLE ' + schema_name + '.' + table_name)
    try:
        cursor.execute(sql)
    except:
        raise
