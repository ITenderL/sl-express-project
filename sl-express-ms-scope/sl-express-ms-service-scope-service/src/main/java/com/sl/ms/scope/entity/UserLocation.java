package com.sl.ms.scope.entity;

import lombok.Data;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author analytics
 * @date 2024/11/3 19:23
 * @description
 */
@Data
@Document("sl_user_location")
public class UserLocation {

    /**
     * 用户id
     */
    @Indexed
    private Long userId;

    /**
     * 位置信息
     */
    @GeoSpatialIndexed(type = GeoSpatialIndexType.GEO_2DSPHERE)
    private GeoJsonPoint location;
}
