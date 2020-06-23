# spark-rn

Algorithms for vectors of small dimension (ℝ^n)"

## WIP

This is very much a work in progress!

Support for K-D Trees implemented on top of Datasets is already available.
They are rather slow but on the other hand they can cope with very large datasets.

I am on the process of adding support for other data structures that I hope would be
more efficient on Spark.

## Example

```scala
import com.github.salva.spark.rn._
import com.github.salva.spark.rn.generator.random.Uniform

val someRandomPoints = Uniform.generateDataset(spark, 1000000, Box.cube(3, 10)) 

val ds = RnSet.autoJoinInBall(someRandomPoints, 0.1)
println(s"number of pairs: ${ds.count}")
```

## Copying

Copyright 2020 Salvador Fandiño (sfandino@yahoo.com)


Licensed under the Apache License, Version 2.0 (the "License");
you may not use the files in this package except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

