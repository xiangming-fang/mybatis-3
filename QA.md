(1) 桥接方法

![桥接方法](imgs/桥接方法.png)

如果一个类继承了一个泛型类或者实现了一个泛型接口，那么编译器在编译这个类的时候会生成一个混合方法，这个混合方法就叫做**桥接方法**
**这个方法是由编译器生成的，方法上有synthetic**
这个方法的作用是：**泛型类型的安全处理。**

(2) 整个 <resultMap> 标签最终会被解析成 ResultMap 对象，它与 ResultMapping 之间的映射关系如下图所示
![ResultMap](imgs/ResultMapping.png)

























