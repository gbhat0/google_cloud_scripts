import bigquery_util
import pandas as pd


class FlattenHandler:


    def __init__(self, table_structure) -> None:

        self.table_structure = table_structure
        self.results = {
            "table_name": [],
            "column_name": [],
            "datatype": [],
            "datatype_mode": [],
            "count_field": [],
            "count_distinct": [],
            "count_null": [],
            "min_count": [],
            "max_count": [],
            "query": []
        }



    def __total_count(self):

        template1 = """
        count(`{field}`) as {field}_total,
        """
        template2 = """       
          COUNTIF(ARRAY_LENGTH(`{field}`) > 0) as {field}_total,
        """

        if type in ["STRING", "INTEGER", "FLOAT", "BOOLEAN"] and self.field_mode == "REPEATED":

            template = template2
        else:

            template = template1    

        self.query += template.format(field=self.field)


    def __distinct_count(self):

        template1 = """
        count(distinct(`{field}`)) as {field}_distinct,
        """
        template2 = """
        NULL as {field}_distinct,
        """

        if type in ["STRING", "INTEGER", "FLOAT", "BOOLEAN"] and self.field_mode == "REPEATED":

            template = template2
        else:

            template = template1    

        self.query += template.format(field=self.field)

    def __null_count(self):

        template = """
        countif(`{field}` is null) as {field}_null,
        """
        self.query += template.format(field=self.field)


    def __min_count(self):

        template1 = """
        cast(min(`{field}`) as string) as {field}_min,
        """
        template2 = """
        NULL as {field}_min,
        """
        if type in ["STRING", "INTEGER", "FLOAT", "BOOLEAN"] and self.field_mode == "REPEATED":

            template = template2
        else:

            template = template1

        self.query += template.format(field=self.field)


    def __max_count(self):

        template1 = """
        cast(max(`{field}`) as string) as {field}_max,
        """
        template2 = """
        NULL as {field}_max,
        """

        if type in ["STRING", "INTEGER", "FLOAT", "BOOLEAN"] and self.field_mode == "REPEATED":

            template = template2
        else:

            template = template1

        self.query += template.format(field=self.field)


    def __store_results(self):

        for item in self.table_structure[self.table_id]['structure']:

            self.results['table_name'].append(self.table_id)
            self.results['column_name'].append(item)
            self.results['datatype'].append(self.table_structure[self.table_id]['structure'][item]['type'])
            self.results['datatype_mode'].append(self.table_structure[self.table_id]['structure'][item]['mode'])
            self.results['count_field'].append(str(self.query_results[item + "_total"].item()).strip())
            self.results['count_distinct'].append(str(self.query_results[item + "_distinct"].item()).strip())
            self.results['count_null'].append(str(self.query_results[item + "_null"].item()).strip())
            self.results['min_count'].append(str(self.query_results[item + "_min"].item()).strip())
            self.results['max_count'].append(str(self.query_results[item + "_max"].item()).strip())
            self.results['query'].append(self.query)
            

    def __start_query_building(self):

        self.query = " SELECT "

        for item in self.table_structure[self.table_id]['structure']:
            self.field = item
            self.field_type = self.table_structure[self.table_id]['structure'][item]['type']
            self.field_mode = self.table_structure[self.table_id]['structure'][item]['mode']
            self.__total_count()
            self.__distinct_count()
            self.__null_count()
            self.__min_count()
            self.__max_count()

        self.query = self.query.strip()[:-1]
        self.query += " FROM " + self.table_id
        self.__execute_querys()
        self.__store_results()
        

    def __execute_querys(self):
        self.query_results = bigquery_util.run_query(self.query, log="""
    ########################
     {0}  
     {1}    
    ########################""".format(self.table_id, self.query))
 

    def get_results(self):
        for item in self.table_structure:

            self.table_id = item

            if self.table_structure[self.table_id]['type_of_table'] != "flatten":

                continue

            self.__start_query_building()

        self.results = pd.DataFrame(self.results)
        return [self.results]
    
