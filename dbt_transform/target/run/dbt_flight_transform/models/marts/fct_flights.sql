

    MERGE INTO "analytics"."main"."fct_flights" AS DBT_INTERNAL_DEST
        USING "fct_flights__dbt_tmp20260101184900728762" AS DBT_INTERNAL_SOURCE
        
            
                
            
        
        ON (DBT_INTERNAL_SOURCE.flight_key = DBT_INTERNAL_DEST.flight_key)
    
    WHEN MATCHED
    THEN
        UPDATE BY NAME
    WHEN NOT MATCHED
        
    THEN
        INSERT BY NAME

  