class CommonQueries:
   
    s3_copy = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}';
    """

    truncate_table = "TRUNCATE TABLE {}"