package com.sl.ms.scope.service.impl;

import cn.hutool.core.util.ObjectUtil;
import com.itheima.em.sdk.EagleMapTemplate;
import com.itheima.em.sdk.vo.Coordinate;
import com.itheima.em.sdk.vo.GeoResult;
import com.sl.ms.scope.entity.ServiceScopeEntity;
import com.sl.ms.scope.entity.UserLocation;
import com.sl.ms.scope.enums.ServiceTypeEnum;
import com.sl.ms.scope.service.ScopeService;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author analytics
 * @date 2024/11/3 17:49
 * @description
 */
@Slf4j
@Service
public class ScopeServiceImpl implements ScopeService {

    @Resource
    private MongoTemplate mongoTemplate;

    @Resource
    private EagleMapTemplate eagleMapTemplate;


    @Override
    public Boolean saveOrUpdate(Long bid, ServiceTypeEnum type, GeoJsonPolygon polygon) {
        Query query = Query.query(Criteria.where("bid").is(bid).and("type").is(type.getCode()));
        ServiceScopeEntity serviceScopeEntity = mongoTemplate.findOne(query, ServiceScopeEntity.class);
        if (ObjectUtil.isNull(serviceScopeEntity)) {
            serviceScopeEntity = new ServiceScopeEntity();
            serviceScopeEntity.setId(new ObjectId());
            serviceScopeEntity.setBid(bid);
            serviceScopeEntity.setType(type.getCode());
            serviceScopeEntity.setPolygon(polygon);
            serviceScopeEntity.setCreated(System.currentTimeMillis());
            serviceScopeEntity.setUpdated(System.currentTimeMillis());
        } else {
            serviceScopeEntity.setPolygon(polygon);
            serviceScopeEntity.setUpdated(System.currentTimeMillis());
        }
        try {
            mongoTemplate.save(serviceScopeEntity);
            return true;
        } catch (Exception e) {
            log.error("保存或修改服务范围失败，参数：{}", serviceScopeEntity, e);
            return false;
        }
    }

    @Override
    public Boolean delete(String id) {

        return null;
    }

    @Override
    public Boolean delete(Long bid, ServiceTypeEnum type) {
        Query query = Query.query(Criteria.where("bid").is(bid).and("type").is(type.getCode()));
        return mongoTemplate.remove(query, ServiceScopeEntity.class).getDeletedCount() > 0;
    }

    @Override
    public ServiceScopeEntity queryById(String id) {
        return null;
    }

    @Override
    public ServiceScopeEntity queryByBidAndType(Long bid, ServiceTypeEnum type) {
        Query query = Query.query(Criteria.where("bid").is(bid).and("type").is(type.getCode()));
        return mongoTemplate.findOne(query, ServiceScopeEntity.class);
    }

    @Override
    public List<ServiceScopeEntity> queryListByPoint(ServiceTypeEnum type, GeoJsonPoint point) {
        Query query = Query.query(Criteria.where("type").is(type.getCode()).and("polygon").intersects(point));
        return mongoTemplate.find(query, ServiceScopeEntity.class);
    }

    @Override
    public List<ServiceScopeEntity> queryListByPoint(ServiceTypeEnum type, String address) {
        GeoResult geoResult = eagleMapTemplate.opsForBase().geoCode(address);
        Coordinate location = geoResult.getLocation();
        return queryListByPoint(type, new GeoJsonPoint(location.getLongitude(), location.getLatitude()));
    }

    /**
     * 查询附近的人的所有用户id
     *
     * @param userId 用户id，中心点用户
     * @param metre  距离，单位：米
     * @return 附近的人
     */
    @Override
    public List<Long> queryNearUser(Long userId, Double metre) {
        // 1、根据用户id，查询用户的位置信息
        Query query = Query.query(Criteria.where("userId").is(userId));
        UserLocation location = mongoTemplate.findOne(query, UserLocation.class);
        if (location == null) {
            return null;
        }
        // 2、以当前用户位置绘制原点
        GeoJsonPoint point = location.getLocation();
        // 3、绘制半径
        Distance distance = new Distance(metre / 1000, Metrics.KILOMETERS);
        // 5、构建查询对象
        NearQuery nearQuery = NearQuery.near(point).maxDistance(distance);
        // 6、执行查询，由近到远排序
        GeoResults<UserLocation> geoResults = mongoTemplate.geoNear(nearQuery, UserLocation.class);
        // 7、获取结果对象，其中userLocationGeoResult.getDistance()可以获取目标点与中心点的位置
        return geoResults.getContent().stream()
                .map(userLocationGeoResult -> userLocationGeoResult.getContent().getUserId())
                .collect(Collectors.toList());
    }
}
