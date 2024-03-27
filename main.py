"""This is the main entry-point into the program"""

import src.make_templates as mt
import src.check as c

if __name__ == '__main__':
    birds = mt.make_birds()
    c.check_birds(birds)
    n = input('How many records would you like to unit test? (Integer required).')
    verbose = input('Do you want to print the results of the unit test to console? (True or False required).')
    c.unit_test(birds, n, verbose)
    birds = c.validate_db(birds, n, verbose)
    