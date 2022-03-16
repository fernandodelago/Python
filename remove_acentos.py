def udf_col_name_standard(df_name):
    '''
    Format all columns of dataframe in lowercase and remove special characters for "_".
    Input.: Spark Dataframe
    Output: Spark Dataframe
    Example how to use:
    df_result = udf_col_name_standard(df_original_data)
    '''
    # library used: import re
    return df_name.toDF(*[re.sub("[^0-9a-zA-Z]+",'_', item_column).lower() for item_column in df_name.columns])
