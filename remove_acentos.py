def udf_col_name_standard(df_name):
    '''
    Format all columns of dataframe in lowercase and remove special characters for "_".
    lib required: pip install unidecode
    Input.: Spark Dataframe
    Output: Spark Dataframe
    Example how to use:
    from unidecode import unidecode
    df_result = udf_col_name_standard(df_original_data)
    '''
    # library used: import re
    new_columns = [unidecode(col_name).lower().replace(' ', '_') for col_name in df.columns]
    renamed_df = df.toDF(*new_columns)
    return renamed_df
