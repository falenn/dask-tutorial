#!/usr/bin/env python
import dask.delayed as delayed
from dask.diagnostics import ProgressBar


def add_two(x):
  return x + 2

def sum_two_numbers(x,y):
  return x + y

def multiply_four(x):
  return x * 4

def main():
    print("dag-1")
    data = [1,5,8,10]
    step1 = [delayed(add_two)(i) for i in data]
    step2 = [delayed(multiply_four)(j) for j in step1]
    total = delayed(sum)(step2)
    total.visualize()
    data2 = [delayed(sum_two_numbers)(k,total) for k in data]
    total2 = delayed(sum)(data2)
    total2.visualize()

if __name__ == "__main__":
    main()
