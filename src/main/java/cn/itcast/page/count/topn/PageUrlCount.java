package cn.itcast.page.count.topn;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PageUrlCount implements Comparable<PageUrlCount>{

    private String url;

    private Integer count;



    public String toString() {
        return this.url + "," + this.count;
    }

    public int compareTo(PageUrlCount o) {
        if (o.count == this.count){
            return this.url.compareTo(o.url);
        }
        return o.count - this.count;
    }
}
