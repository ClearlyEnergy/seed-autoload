# Development Installation
While this module could be installed through `pip install`, it is more convienient for development to link a local copy of the module directory into the seed directory. This lets local and remote changes to be quickly incorperted into a running instance of the seed server.
Instructions are include in the HELIX README. In short, execute `ln -s ../seed-autoload/autoload autoload` from the root of the seed repository.
# Usage
* Instantiate the autoload class with a valid user and valid organization for that user. 
* Call autoload_file method passing as arguments csv formated data, a valid import_record instance, a valid cyle, and a column mapping for the data. 

Sample Data:
```
Address,City
123 Test rd, Baltimore
456 Test rd, Balimore
```


Sample Mapping:
```
       [{"from_field": "Address",
         "to_field": "address_line_1",
         "to_table_name": "PropertyState",
        },
        {"from_field": "City",
         "to_field": "city",
         "to_table_name": "PropertyState",
        }],
```
