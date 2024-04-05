"""This is the main workflow for building `birds` from-source, loading to database, and validating each step"""

import src.make_templates as mt
import src.check as c
import src.load_tbls as loader

if __name__ == '__main__':
    birds = mt.make_birds()
    c.check_birds(birds)
    n = input('How many records would you like to unit test? (Integer required).')
    verbose = input('Do you want to print the results of the unit test to console? (True or False required).')
    c.unit_test(birds, n, verbose)
    permission = input('Do you want to load birds to db now? y/n')
    if permission.lower() == 'y':
        loader.load_tbl(birds)
    birds = c.validate_db(birds, n, verbose)
    