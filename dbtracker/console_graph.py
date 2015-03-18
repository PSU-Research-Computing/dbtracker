stats = {'heep': 30, 'bloop': 100, 'bleep': 50, 'blop': 23}


big = max(stats.iterkeys(), key=(lambda key: stats[key]))
print(big)
