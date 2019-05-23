class LatencyMetrics:
  def __init__(self, name, min=0, max=0, mean=0, median=0):
    self.name = name
    self.min = min
    self.max = max
    self.mean = mean
    self.median = median

  def printResult(self):
    print(self.name + ": min: " + str(self.min) + " max: " + str(self.max) + " mean: " + str(self.mean) + " median: " + str(self.median) )


