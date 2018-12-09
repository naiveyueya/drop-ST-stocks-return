import pandas as pd
import numpy as np
import re
import gc

def takemonth(x):
    gc.collect()
    pattern = re.compile('.*/\d+/')
    m = re.search(pattern, x)
    xnew = x[0:(m.end()-1)]
    return xnew