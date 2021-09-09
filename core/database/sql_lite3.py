import sqlite3
from os import path, getcwd


class Database:

    BASE_DIR = path.abspath(getcwd() + "\database")
    connection = None
    cursor = None
    name = None
    path = None

    def __init__(self, name, run_template=False):
        """Database Constructor"""

        self.name = name
        self.path = path.abspath(f"{self.BASE_DIR}\db\{self.name}.db")

        if self.name is not None:
            self.__prepare_database(run_template=run_template)

    def perform_operation(function):
        """ Perform operation decorator

            Used to catch database related exceptions and
            close the database connection at the end of every action.

        """
        def wrapper(*args, **kwargs):

            try:

                args[0].__connect()
                result = function(*args, **kwargs)

            except sqlite3.Error as error:
                print(f"There was a problem with the Database connection.\n {error}")

            except Exception as e:
                print(f"An unexpected error occurred.\n {e}")

            finally:

                if result:
                    if "SELECT" in kwargs['query']:
                        return result.fetchall()
                    else:
                        args[0].connection.commit()

                if args[0].connection:
                    args[0].connection.close()

                return result

        return wrapper

    def all(self, table_name):
        """Selects all data from the database table

            Args:
                table_name: str

            Returns:
                SQL Cursor
        """
        query = f"SELECT * FROM {table_name}"
        return self.query(query=query)

    def byId(self, table_name, id_field, data=None):
        """Selects data from the database table using id field

            Args:
                table_name: str
                id_field: str
                data: dict

            Returns:
                dict
        """
        query = f"SELECT * FROM {table_name} WHERE {id_field} = (?) LIMIT 1"
        return self.query(query=query, parameters=data)

    def where(self, table_name, data):
        """Selects data from the database table with conditions

            Args:
                table_name: str
                data: dict

            Returns:
                dict
        """
        query_data = self.prepare_query_data(data=data, query_type="SELECT")
        query = f"SELECT * FROM {table_name} WHERE {query_data['fields']}"
        return self.query(query=query, parameters=query_data["parameters"])

    def insert(self, table_name, data):
        """Inserts data to the database table

            Args:
                table_name: str
                data: dict

            Returns:
                int: inserted_id
        """
        query_data = self.prepare_query_data(data=data, query_type="INSERT")
        query = f"INSERT OR IGNORE INTO {table_name} ({query_data['fields']}) VALUES ({query_data['fields_placeholder']})"
        result = self.query(query=query, parameters=query_data["parameters"])
        return result.lastrowid

    def update(self, table_name, data, where_data):
        """Updates data from database table

            Args:
                table_name: str
                data: dict
                where_data: dict

            Returns:
                int: affected rows
        """
        update_data = self.prepare_query_data(data=data, query_type="UPDATE")
        conditions_data = self.prepare_query_data(data=where_data, query_type="SELECT")
        update_data['parameters'].extend(conditions_data['parameters'])
        query = f"UPDATE {table_name} SET {update_data['fields']} WHERE {conditions_data['fields']}"
        result = self.query(query=query, parameters=update_data['parameters'])
        return result.rowcount > 0

    def delete(self, table_name, data):
        """Deletes data from database table

            Args:
                table_name: str
                data: dict

            Returns:
                int: affected rows
        """
        query_data = self.prepare_query_data(data=data, query_type="SELECT")
        query = f"DELETE FROM {table_name} WHERE {query_data['fields']}"
        result = self.query(query=query, parameters=query_data['parameters'])
        return result.rowcount > 0

    def exists(self, table_name, data):
        """Checks if data exists in the database table

            Args:
                table_name: str
                data: dict

            Returns:
                bool
        """
        query_data = self.prepare_query_data(data=data, query_type="SELECT")
        query = f"SELECT COUNT(*) AS counter FROM {table_name} WHERE {query_data['fields']} LIMIT 1"
        result = self.query(query=query, parameters=query_data['parameters'])
        return result[0]["counter"] > 0

    def __connect(self):
        """Connect to database"""

        self.connection = sqlite3.connect(self.path, uri=True)
        self.connection.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        self.cursor = self.connection.cursor()

    def __prepare_database(self, run_template=False):
        """Connects to database and runs database template

            Args:
                 run_template: bool
        """
        try:
            self.__connect()

            if run_template:
                try:
                    tables_file = open("{}/structs/tables_{}.sql".format(self.BASE_DIR, self.name))
                    tables_as_string = tables_file.read()
                    self.cursor.executescript(tables_as_string)
                except:
                    pass

                try:
                    inserts_file = open("{}/structs/inserts_{}.sql".format(self.BASE_DIR, self.name))
                    inserts_as_string = inserts_file.read()
                    self.cursor.executescript(inserts_as_string)
                except:
                    pass

        except sqlite3.OperationalError as e:
            print(e)

        finally:
            if self.connection:
                self.connection.close()

    @perform_operation
    def query(self, query, parameters=None):
        """ Executes query on the database

            Args:
                query: str
                parameters: list

            Returns:
                SQL Cursor
        """
        if parameters is not None:
            return self.cursor.execute(query, parameters)
        else:
            return self.cursor.execute(query)

    def prepare_query_data(self, data, query_type="SELECT"):
        """ Generates variables from data ready for query

            Args:
                data: dict
                query_type: str

            Returns:
                fields, parameters or fields_placeholder, fields, parameters
        """
        fields = str()
        fields_placeholder = str()
        parameters = list()
        counter = 0

        fields_rule = {
            "SELECT": {"fields": ["{} = (?)", "{} = (?) AND "]},
            "UPDATE": {"fields": ["{} = ?", "{} = ?, "]},
            "INSERT": {"fields": ["{}", "{}, "], "placeholder": ["?", "?,"]},
        }.get(query_type)

        for key, value in data.items():

            if counter < len(data) - 1:
                fields += fields_rule.get("fields")[1].format(key)
                if query_type not in ["SELECT", "UPDATE"]:
                    fields_placeholder += fields_rule.get("placeholder")[1]
            else:
                fields += fields_rule.get("fields")[0].format(key)
                if query_type not in ["SELECT", "UPDATE"]:
                    fields_placeholder += fields_rule.get("placeholder")[0]

            parameters.append(value)
            counter += 1

        query_data = {
            "fields": fields,
            "parameters": parameters,
            "fields_placeholder": fields_placeholder if len(fields_placeholder) > 0 else None,
        }

        return query_data

