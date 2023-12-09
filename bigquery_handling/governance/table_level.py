import bigquery_util


class TableLevel:


    def __init__(self, table_structure) -> None:

        self.table_structure = table_structure
        

    def __build_query(self):
        template1 = """select 

        '{tbl}' as table_name,
        '{pt}' as partition_column,
        count(1) as count_table, 
        cast(min(`{pt}`) as string) as min_partition_column,
        cast(max(`{pt}`) as string) as max_partition_column,       
        FROM {tbl}
        """
        template2 = """select 
        '{tbl}' as table_name,
        "" as partition_column,
        count(1) as count_table, 
        NULL as min_partition_column,
        NULL as max_partition_column,
        
        FROM {tbl}
        """
        
        if getattr(self.table_structure[self.table_id]['table_obj'], "time_partitioning", None):

            pt = self.table_structure[self.table_id]['table_obj'].time_partitioning.field
            query = template1.format(tbl=self.table_id, pt=pt)
        else:

            query = template2.format(tbl=self.table_id)
        
        self.query += query


    def get_results(self):
        
        self.query = ""

        for index, table in enumerate(self.table_structure):
            
            self.table_id = table 
            self.__build_query()

            if index != len(self.table_structure)-1:

                self.query += " \n UNION ALL \n "

        self.query = self.query.strip()

        return bigquery_util.run_query(
            query=self.query,
            log="""
            ###################### 
            Table Level Query         
            {0}
            """.format(self.query)
        )
        

