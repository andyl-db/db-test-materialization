import parse_args
from dbruntime import UserNamespaceInitializer

def main():
    """
    This program will take in the following arguments:
      --securable
          Securable that needs to be materialized
      --projection_selection_clause
          Selects specific columns from the securable
      --row_selection_clause
          Predicates pushdown clause used to filter the rows
      --location
          The location the materialization will be stored
    
    It will then read the securable, apply projection selection and row selection clauses if available, then write it to the specified storage location.
    """
    
    args = parse_args()
    user_namespace_initializer = UserNamespaceInitializer.getOrCreate()
    spark = user_namespace_initializer.namespace_globals["spark"]

    # Read the securable
    securable = args["securable"]
    print(f"Attempting to materialize {securable} to location")

    print(f"Reading {securable}")
    data_frame = spark.read.table(securable)

    # Apply projection selection clause if specified
    if "projection_selection_clause" in args:
        projection_selection_clause = args["projection_selection_clause"]
        print(f"Applying {projection_selection_clause} to dataframe")
        data_frame = data_frame.select(projection_selection_clause)

    # Apply row selection clause if specified
    if "row_selection_clause" in args:
        row_selection_clause = args["row_selection_clause"]
        print(f"Applying {row_selection_clause} to dataframe")
        data_frame = data_frame.filter(args["row_selection_clause"])

    # Save the materialization
    print("Saving materialization...")
    location = args["location"]
    data_frame.write.format("delta").save(location)
    
if __name__ == '__main__':
    main()
