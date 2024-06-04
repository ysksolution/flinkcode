package chapter05_watermark.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class AdsClickLog {
    private long userId;
    private long adsId;
    private String province;
    private String city;
    private Long timestamp;

}