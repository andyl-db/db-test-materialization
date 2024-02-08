import sys
import uuid
import collections.abc
from databricks.connect import DatabricksSession

def main():
    # Parse arguments
    # Repeated arguments will be grouped into an array
    """
        --host
            Databricks host
        --token
            Databricks PAT token
        --securable
            Securable that needs to be materialized
        --hadoop
            Hadoop configuration to set
        --location
            The location the materialization will be stored
    """
    args = {}
    for arg in sys.argv[1:]:
        sp = arg.split("=", 1)
        key = sp[0]
        if key.startswith("--"):
            key = key[2:]
        val = sp[1]
        if key in args:
            if not isinstance(args[key], collections.abc.Sequence):
                args[key] = [args[key]]
            args[key].append(val)
        else:
            args[key] = val

    # Setup Spark connect
    host = args["host"]
    token = args["token"]
    securable = args["securable"]
    s = DatabricksSession.builder.host(host).token(token).header('x-databricks-session-id', str(uuid.uuid4())).getOrCreate()
    df = s.read.table(securable)

    # Set Hadoop configuration for writing the materialization
    if "hadoop" in args:
        for conf in args["hadoop"]:
            sp = conf.split("=", 1)
            key = sp[0]
            val = sp[1]
            s.conf.set(key, val)

    # Save the materialization
    # df.write.format("delta").save(args["location"])
    df.show()
    
if __name__ == '__main__':
    main()
