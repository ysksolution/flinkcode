package chapter05_watermark.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by Smexy on 2022/10/19
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordCount
{
    private String word;
    private Integer count;
}
