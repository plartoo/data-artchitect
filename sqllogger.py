class SQLLogger:
    def __init__(self, log_table_name, schema_name):
        self.log_table_name = log_table_name
        self.schema_name = schema_name

    def log(self, cursor, description, note):
        sql = (
        'INSERT INTO ' + self.schema_name + '.' + self.log_table_name +
        ' VALUES (\'' + description + '\', GETDATE(), \'' + note
        + '\'); COMMIT;')

        try:
            cursor.execute(sql)
            # print(sql)
        except:
            raise