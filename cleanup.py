import parse_args

def main():
    """
    This program will take in the following arguments:
      --location
        The location the materialization
    
    It will then delete the specified materialization storage location.
    """
    args = parse_args()
    dbutils.fs.rm(dir = args["location"], recurse = True)
    
if __name__ == '__main__':
    main()
