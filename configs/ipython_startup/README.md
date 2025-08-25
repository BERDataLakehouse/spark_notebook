* These ipython startup scripts are executed when you start an IPython or Jupyter Notebook server.
* They are executed in alphabetical order, so you can use prefixes like `01-`, `02-`, etc. to control the order of execution.
* These will fail silently as the notebook prioritizes startup rather than erroring out.
* You will see a single log warning in the notebook logs if there is an error in the startup script, without any further details

#TODO:
* We should put this in a module, so they can load dependencies
* If we later delete ipython startup scripts, the old ones will still be there, so we should have a cleanup script?