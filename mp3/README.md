## Contributed project
We are using MP2 Best Solutions from group 57. (Thanks!)
We are borrowing our old MP1 for debugging (i.e. grep logs)

## Language
We are using Python 2, so no compilation is required.  

## Execution
Start by calling `python main.py`  
An introducer (either *fa17-cs425-g48-01.cs.illinois.edu* or *fa17-cs425-g48-02.cs.illinois.edu*) is required to join first in order to make new node join.  
Here is the complete instruction set:  
```
delete <filename> : delete file in DFS  
get    <filename> : download file from DFS  
help              : display possible cmds  
join              : join the group  
leave             : leave the group  
ls     <filename> : list all VMs where file is replicated  
memb              : list the membership list  
misc              : display misc info  
put    <filename> : upload file to DFS  
self              : list self's id  
store             : list the set of filenames stored here  
```
  
There are also many arguments provided, mainly for debugging purpose  
Press Ctrl-C to simulate a failure

## Files
#### **main.py**
Uses MP2 Best Solutions from group 57 as the basis  
Merging dfs with their origin code and instructions.

#### **dfs.py**
An distributed file system model, initialized after joining the group  
Uses monitor to check upcoming messages and perform corresponding operations  

#### **message.py**
Provides methods for sending receiving messages or file content  
Uses json to encode and decode messages