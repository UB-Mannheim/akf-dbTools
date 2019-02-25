![akf-dbTools](docs/img/Tools.png "akf-dbTools")
========================
Overview
------------
**akf-dbTools** can modify or update certain parts of the database.
It is part of the [Aktienführer-Datenarchiv work process][akf-link] and 
customized to the needs of this project.

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
    $ python ./akf_dbTools.py --input "./db.sqlite" --tools 3 

This will perform the KennGetter-Option on a database called "db".

    # See --help for more informations
    $ python ./akf_dbTools.py --help

Tool description
----------------
At the moment the program contains four different tools:

#### 0. Json2sqlite for books  
It parses the json information,  
which are gained from the books,  
to the sqlite database.  
*RefGetter and KennGetter  
are recommended to use afterwards.*

#### 1. Json2sqlite for cds  
It parses the json information,  
which are gained from the cds,  
to the sqlite database.  
*RefGetter and KennGetter  
are recommended to use afterwards.*

#### 2. RefGetter  
It gets the 'referenz' values, reads all years which  
are bind to it and pretty prints the data as year span 
into the `Main` table in the `Jahrespanne` column.

#### 3. KennGetter 
It gets the 'referenz' values, reads all WKN/ISIN which    
are bind to it and prints the unique data into the   
`Main` table in the `Kennnummer` column.

Copyright and License
--------

Copyright (c) 2017 Universitätsbibliothek Mannheim

Author: 
 * [Jan Kamlah](https://github.com/jkamlah)

**db-Tools** is Free Software. You may use it under the terms of the Apache 2.0 License.
See [LICENSE](./LICENSE) for details.


[akf-link]:  https://github.com/UB-Mannheim/Aktienfuehrer-Datenarchiv-Tools "Aktienfuehrer-Datenarchiv-Tools"



