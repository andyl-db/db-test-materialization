import sys
import collections.abc

def main():
    # Parse arguments from sys.argv into dictionary
    # Repeated arguments will be grouped into an array
    """
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

    dbfsutil.fs.rm(args["location"])
    
if __name__ == '__main__':
    main()
