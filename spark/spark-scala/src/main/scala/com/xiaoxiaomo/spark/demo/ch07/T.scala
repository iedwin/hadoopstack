package com.xiaoxiaomo.demo.ch07

/**
 * 找不到scala.collection
 * Created by xiaoxiaomo on 2016/3/30.
 */
package com {
    package horstmann {
        package impatient {
            class Manager {
                // 因为scala包总是被引入，所以下面的collection包实际是指scala.collection
                //val subordinates = new collection.mutable.ArrayBuffer[String]
            }
        }
    }
}

//如果再有一个包这样定义，上面
//new collection.mutable.ArrayBuffer[String]
package com {
    package horstmann {
        package collection {
        //....
        }
    }
}


//package com.xiaoxiaomo.demo
//package ch07
//
//import com.xiaoxiaomo.demo.ch07
//
//class demo{
//    //info
//}
//
//
//// 上面的等同于
//package com.xiaoxiaomo.demo {
//package ch07 {
//class demo{
//    //info
//}
//
//}
//}

