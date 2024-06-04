package chapter05_watermark.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by Smexy on 2022/10/25
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyAcc
{
    private Integer count = 0;
    // int sum = 0
    // Integer sum = null
    private Integer sum = 0;

}
