import sys
import collections.abc

def parse_args():
  """
  Parse arguments from sys.argv into dictionary.
  Repeated arguments will be grouped into an array.

  For example, "--foo=bar --foo=party --bar=foo" will generate the following:
    dict(
      "foo": ["bar", "party"],
      "bar": "foo"
    )
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
  return args