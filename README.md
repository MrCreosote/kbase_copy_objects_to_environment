## Copy KBase objects / samples from one environment to another

Extremely nasty and inefficient means to copy KBase data objects and samples
from environment to environment.

Object copying only works for Genomes and Assemblies at the moment.

### Dependencies

* `pip install requests`

### Object copy usage

* Update the script to set source and target workspaces and whether to start from Genomes
  (which will also copy their assemblies) or Assemblies.

```
python copy_objects_to_env.py <source env token file> <target env token file>
```

The token files have a KBase token on the first line and nothing else.

### Sample copy usage

* Will only work for workspaces that were copied using the Object copy script (and therefore
  have the copy source saved in the object metadata.)
* Update the script to set source and target workspaces and the object type to process.
* The script uses a concordance file to map samples in one environment to samples in another.
  **ALWAYS** use the same condordance file for a workspace to workspace copy or duplicate
  samples will result.

```
python copy_samples_to_env.py <sample concordance file> <source env token file> <target env token file>
```

The token files have a KBase token on the first line and nothing else.

### TODO

* This code is garbage, it needs a full workover before anyone should ever use it for anything
* Duplicated code between the 2 scripts