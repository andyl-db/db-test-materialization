import sys
import collections.abc
from dbruntime import UserNamespaceInitializer

def main():
    # Parse arguments from sys.argv into dictionary
    # Repeated arguments will be grouped into an array
    """
        --securable
            Securable that needs to be materialized
        --projection_selection_clause
            Selects specific columns from the securable
        --row_selection_clause
            Predicates pushdown clause used to filter the rows
        --hadoop
            Hadoop configuration to set
        --location
            The location the materialization will be stored
    """
    args = {}
    for arg in sys.argv[1:]:
        split = arg.split("=", 1)
        key = split[0]
        if key.startswith("--"):
            key = key[2:]
        val = split[1]
        if key in args:
            if not isinstance(args[key], collections.abc.Sequence):
                args[key] = [args[key]]
            args[key].append(val)
        else:
            args[key] = val

    user_namespace_initializer = UserNamespaceInitializer.getOrCreate()
    spark = user_namespace_initializer.namespace_globals["spark"]

    # Read the securable
    securable = args["securable"]
    data_frame = spark.read.table(securable)
    
    # Apply projection selection clause if specified
    if "projection_selection_clause" in args:
        data_frame = data_frame.select(args["projection_selection_clause"])

    # Apply row selection clause if specified
    if "row_selection_clause" in args:
        data_frame = data_frame.filter(args["row_selection_clause"])

    data_frame.show()
    
if __name__ == '__main__':
    main()
