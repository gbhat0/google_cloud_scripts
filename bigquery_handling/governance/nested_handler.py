import  bigquery_util

class QueryTemplates:

    
    template1 = """
    select 
    "{tbl}" as table_name,
    "{col}" as column_name, 
    '{datatype}' as datatype,
    '{mode}' as datatype_mode,
    count(`{field}`) as count_field, 
    count(distinct(`{field}`)) as count_distinct, 
    countif(`{field}` is null) as count_null,
    cast(min(`{field}`) as string) as min_count, 
    cast(max(`{field}`) as string) as max_count
    from {tbl}
    {unnesting}
    """ # NORMAL 

    template2 = """
    select 
        "{tbl}" as table_name,
        "{col}" as column_name,
        '{datatype}' as datatype,
        '{mode}' as datatype_mode,
        count(`{field}`) as count_field,
        NULL as count_distinct,
        countif(`{field}` is null) as count_null,
        NULL as min_count,
        NULL as max_count
      
    from {tbl}
    {unnesting}
    """ # -> REOCRD NULLABLE, RECORD REPEATED

    template3 = """ 
    select
      "{tbl}" as table_name,
      "{col}" as column_name,
      '{datatype}' as datatype,
      '{mode}' as datatype_mode, 
      COUNTIF(ARRAY_LENGTH(`{field}`) > 0) as count_field,
      NULL as count_distinct, 
      countif(`{field}` is null) as count_null,
      NULL as min_count,
      NULL as max_count
    from {tbl}
    {unnesting}
    """  # -> STRING, INTEGFR, FLOAT, BOOLEAN  REPEATED



class NestedHandler:
    """
    """
    def __init__(self,  table_structure) -> None:

        self.table_structure = table_structure
        self.results = []
        

    def __unnester(self, field):

        unnesting = ""
        unnest_chain = []

        for index, item in enumerate(field.split(".")):

            if index == 0:

                column = item
            else:

                column = column + "." + item            

            if self.table_structure[self.table_id]['structure'][column]['type'] == "RECORD" and self.table_structure[self.table_id]['structure'][column]['mode'] == "REPEATED":

                unnest_name = item + "_nest"
                left_join = ".".join(unnest_chain)+"."+item
    
                if left_join.endswith("."):

                    left_join = left_join[:-1]

                if left_join.startswith("."):

                    left_join = left_join[1:]
                
                unnesting += "\nLEFT JOIN UNNEST({0}) {1}".format(left_join, unnest_name)
                unnest_chain = [unnest_name]
            else:    

                unnest_chain.append(item)

        if not unnest_chain:

            unnest_chain.append(column)

        return unnest_chain, unnesting

    
    def __query_builder(self, field, type, mode):

        unnest_chain, unnesting = self.__unnester(field)

        if type in ["STRING", "INTEGER", "FLOAT", "BOOLEAN"] and mode == "REPEATED":

            template = QueryTemplates.template3
        else:

            template = QueryTemplates.template1

        temp_field = ".".join(unnest_chain)
        query  = template.format(
            tbl=self.table_id,
            field=temp_field,
            unnesting=unnesting,
            col=field,
            datatype=type,
            mode=mode
            ) 
        self.results.append(bigquery_util.run_query(query, log="""
        #######################
        Running Query for {0}  {1}
        {2}
        """.format(field, self.table_id, query)))


    def __start_query_building(self):

        for item in self.table_structure[self.table_id]['structure']:
        
            if self.table_structure[self.table_id]['structure'][item]['type'] in ["RECORD"] and  self.table_structure[self.table_id]['structure'][item]['mode'] in ["NULLABLE", "REPEATED"]:
                continue
            
            self.__query_builder(item, self.table_structure[self.table_id]['structure'][item]['type'], self.table_structure[self.table_id]['structure'][item]['mode'])
        
        
    def get_results(self):
        
        for table in self.table_structure:
    
            if self.table_structure[table]['type_of_table'] != "nested":
                continue
            
            self.table_id = table
            self.__start_query_building()        
        
        return self.results


