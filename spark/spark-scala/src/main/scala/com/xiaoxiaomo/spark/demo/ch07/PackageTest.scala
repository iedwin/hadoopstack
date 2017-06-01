/**
 * 包
 * Created by xiaoxiaomo on 2016/3/30.
 */
package com.xiaoxiaomo.demo.ch07{
    package test{

        //类在：com.xiaoxiaomo.demo.ch07.test下
        class PackageTest {
            //对象
            object ObjectTest{
                //方法
                def test( num : Int ) {
                    println("num:"+num)
                }
            }
        }

        object ObjectTest{
            def ObjTestMethod{
                println("Hello!")
            }
        }

        package test2{
            object ObjTestMethod2{
                //可以直接调用上级方法
                ObjectTest.ObjTestMethod
            }
        }
    }
}

package com {

import com.xiaoxiaomo.demo.ch07.test.{PackageTest, ObjectTest}

object PackageCom{
    //test
    def main(args: Array[String]) {

        //调用com.xiaoxiaomo.demo.ch07.test下

        //1、ObjectTest的ObjTestMethod方法
        ObjectTest.ObjTestMethod ;
        //2、PackageTest类的ObjectTest对象的test方法
        new PackageTest().ObjectTest.test(888)
    }
}
}





