## Copy KBase objects from one environment to another

Extremely nasty and inefficient means to copy KBase data objects from environment to environment.

Only works for Genomes and Assemblies at the moment.

### Usage

* Install requests
* Update the script to set source and target workspaces and whether to start from Genomes
  (which will also copy their assemblies) or Assemblies.

```
python copy_objects_to_env.py <source env token file> <target env token file>
```

The token files have a KBase token on the first line and nothing else.

### TODO

* This script is garbage, it needs a full workover before anyone should ever use it for anything