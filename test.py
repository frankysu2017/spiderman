#!/usr/bin/env python3
# coding=utf-8

from functools import reduce
import pandas as pd
if __name__ == '__main__':
    lst = [[None]*6, [1,2,3,4,5,6]]
    df = pd.DataFrame(lst, columns=['所属城市', '所属城市英文', '机场名称', '机场英文名称', '三字代码', '四字代码'])
    print(df)