These hooks get executed when the Jupyter Docker Stacks are built.
They will get copied in the dockerfile to /usr/local/bin/before-notebook.d
They run in alphabetical order, so you can use the prefix `01-`, `02-`, etc. to control the order of execution, starting at 11