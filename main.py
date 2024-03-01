"""This is the main entry-point into the program"""

import src.make_templates as mt
import src.check as c

if __name__ == '__main__':
    birds = mt.make_birds()
    c.check_birds(birds)
    