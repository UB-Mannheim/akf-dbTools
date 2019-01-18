![akf-dbTools](doc/img/Tools.png)
========================
Overview
------------
The **akf-dbTools** can modify or update certain parts of the database.

Building instructions
--------------------
Method using Conda:

    $ conda create -n dbtools_env python=3.6 
    $ source activate dbtools_env  
    $ conda install --file requirements.txt 
    $ python setup.py install  

Running
-------
Example:

    # perform KennGetter-Tool
    $ python ./akf_dbTools.py --input "./db.sqlite" --tools 0 

This will create a directory "out/..." containing all cropped
segments and debug outputs. And a subdirectory "spliced/.."
containing the final spliced image.

    # See --help for more informations
    $ python ./akf_dbTools.py --help

Tool description
----------------
At the moment the program contains two different tools:

#### json2sqlite for books  
It parses the json information, 
which are gained from the books,
to the sqlite database.
*RefGetter* and *KennGetter* 
are *not* necessary to use afterwards.

#### json2sqlite for cds  
It parses the json information, 
which are gained from the cds,
to the sqlite database.
*RefGetter* and *KennGetter* 
are necessary to use afterwards.

#### RefGetter  
It gets the 'referenz' values, reads all years which  
are bind to it and pretty prints the data into the   
table 'Main' in the column 'Jahrespanne'.

#### KennGetter 
It gets the 'referenz' values, reads all WKN/ISIN which    
are bind to it and prints the data into the   
table 'Main' in the column 'Kennnummer'. 



