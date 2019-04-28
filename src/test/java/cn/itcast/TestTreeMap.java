package cn.itcast;

import cn.itcast.page.count.topn.PageUrlCount;
import cn.itcast.pagecount2.urltopn.PageBean;
import org.junit.Test;

import java.util.ArrayList;
import java.util.TreeMap;

public class TestTreeMap {

    @Test
    public void f(){
        TreeMap<String, Integer> treeMap = new TreeMap<>();

        treeMap.put("cc", 13);
        treeMap.put("ab", 24);
        treeMap.put("a", 12);
        treeMap.put("b", 1);
        treeMap.put("ba", 19);

        System.out.println(treeMap);


        /*TreeMap<PageUrlCount, Object> treeMap1 = new TreeMap<>();

        PageUrlCount pageUrlCount1 = new PageUrlCount("163.com", 20);
        PageUrlCount pageUrlCount2 = new PageUrlCount("qq.com", 10);
        PageUrlCount pageUrlCount3 = new PageUrlCount("baidu.com", 20);
        PageUrlCount pageUrlCount5 = new PageUrlCount("ba.com", 20);
        PageUrlCount pageUrlCount4 = new PageUrlCount("singa.com", 30);

        treeMap1.put(pageUrlCount1, null);
        treeMap1.put(pageUrlCount2, null);
        treeMap1.put(pageUrlCount3, null);
        treeMap1.put(pageUrlCount4, null);
        treeMap1.put(pageUrlCount5, null);
        for (PageUrlCount pageUrlCount : treeMap1.keySet()) {
            System.out.println(pageUrlCount);
        }*/

    }

    @Test
    public void sub(){
        System.out.println("heloo".substring(0, 3));
    }


    @Test
    public void join(){
        String a = "xx";
        a += "oo";
        System.out.println(a);
    }

    @Test
    public void addToList(){
        ArrayList<PageBean> list = new ArrayList<>();

        PageBean pageBean = new PageBean();
        pageBean.setTimes(3);
        pageBean.setUrl("aaa");
        list.add(pageBean);

        pageBean.setTimes(4);
        pageBean.setUrl("bbb");
        list.add(pageBean);

        System.out.println(list);
    }

}
