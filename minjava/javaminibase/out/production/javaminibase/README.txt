Added get and set methods in order to perform the respective functionalities of interval data type
./heap/Tuple.java

modified page get and set method to support intervals
./global/Convert.java

A Anew attribute type called attribute interval to support interval based joins
./global/AttrType.java

Operand defination were modified in order support new operations for intervals such as containment and enclosure
such that they return
./iterator/Operand.java

Tupple comparision methods were d in order to support between interval datatypes
./iterator/TupleUtils.java

The sort was modified to accomodate soting intervals containing tuples
./iterator/Sort.java

Modified Eval expression comparision
./iterator/PredEval.java

Condtional Expressions were modified in order to support join operation between interval datatypes
./iterator/CondExpr.java


New Data type name ”intervaltype” was added consisting of 2 integers, start and end .
./global/intervaltype.java




















