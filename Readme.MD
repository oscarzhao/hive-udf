# CSE UDF

User Defined Function, 自定义函数

狭义上的UDF 分为三类:

UDF: one in, one out, e.g. regexp_replace, md5, etc;
UDAF: N in, out out, e.g. sum/max/min/avg, etc;
UDTF: one in, N out, e.g. explode

## Java类命名方式

为了便于在使用时区分出来，对类命名进行一些约束：
如果是 UDF，类名以 'UDF'结尾; 如果是 UDAF，类名以 'UDAF' 结尾

## 如何写自定义函数

### UDF 有两种写法：

1. extends UDF: 输出是基本数据类型

   - bigint -> Long
   - bool -> Boolean
   - string -> String
   - array<T> -> ArrayList<T>
   - map<K, V> -> HashMap<K, V>

优点：写起来非常简单

4. extends GenericUDF: 输出里包含 struct

优点：功能强大


### UDAF 有一种写法

